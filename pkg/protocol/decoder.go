package protocol

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DecodeV1 decodes a V1 (or V2) GPS location frame.
//
// V1 field layout (f.Fields after CMD):
//
//	[0] HHMMSS      — UTC time
//	[1] A | V       — A = GPS fix valid, V = no fix
//	[2] Lat         — DDDMM.MMMM (NMEA degrees+minutes)
//	[3] N | S       — hemisphere
//	[4] Lon         — DDDMM.MMMM
//	[5] E | W       — hemisphere
//	[6] Speed       — knots (converted to km/h on output)
//	[7] Course      — degrees 0–359
//	[8] DDMMYY      — UTC date
//	[9] Flags       — 8-char hex bitmask (optional)
func DecodeV1(f *Frame) (*LocationReport, error) {
	if len(f.Fields) < 9 {
		return nil, fmt.Errorf("h02: V1 requires 9+ fields, got %d", len(f.Fields))
	}

	timeStr := f.Fields[0]
	gpsFixed := strings.EqualFold(f.Fields[1], "A")
	latStr := f.Fields[2]
	ns := f.Fields[3]
	lonStr := f.Fields[4]
	ew := f.Fields[5]
	spdStr := f.Fields[6]
	crsStr := f.Fields[7]
	dateStr := f.Fields[8]

	ts, err := parseDateTime(dateStr, timeStr)
	if err != nil {
		return nil, fmt.Errorf("h02: V1 datetime: %w", err)
	}

	var lat, lon float64
	if gpsFixed {
		lat, lon, err = parseLatLon(latStr, ns, lonStr, ew)
		if err != nil {
			return nil, fmt.Errorf("h02: V1 coords: %w", err)
		}
	}

	speedKnots, _ := strconv.ParseFloat(spdStr, 64)
	speedKMH := speedKnots * 1.852

	course, _ := strconv.ParseFloat(crsStr, 64)

	var rawFlags string
	var accOn bool
	var alarmType uint8
	if len(f.Fields) > 9 {
		rawFlags = strings.TrimSpace(f.Fields[9])
		accOn, alarmType = parseFlags(rawFlags)
	}

	return &LocationReport{
		IMEI:      f.IMEI,
		Timestamp: ts,
		Latitude:  lat,
		Longitude: lon,
		Speed:     speedKMH,
		Course:    course,
		GPSFixed:  gpsFixed,
		ACCOn:     accOn,
		AlarmType: alarmType,
		RawFlags:  rawFlags,
	}, nil
}

// DecodeNBR decodes an NBR (LBS-only) frame.
// Returns a LocationReport with GPSFixed=false and no coordinates.
// The raw frame is passed through for consumer-side cell tower lookup.
func DecodeNBR(f *Frame) (*LocationReport, error) {
	if len(f.Fields) < 1 {
		return nil, fmt.Errorf("h02: NBR body empty")
	}
	// NBR: *HQ,IMEI,NBR,HHMMSS,MCC,MNC,(LAC,CID,Signal)+,DDMMYY,Flags#
	// Minimum 8 fields; date is second-to-last, flags is last.
	timeStr := f.Fields[0]
	var dateStr string
	if len(f.Fields) >= 2 {
		dateStr = f.Fields[len(f.Fields)-2]
	}
	ts, _ := parseDateTime(dateStr, timeStr)

	return &LocationReport{
		IMEI:      f.IMEI,
		Timestamp: ts,
		GPSFixed:  false,
	}, nil
}

// DecodeLINK decodes a LINK (signal/status) frame.
// LINK: *HQ,IMEI,LINK,HHMMSS,GSMSignal,SatCount,Voltage,Armed,DDMMYY#
func DecodeLINK(f *Frame) (*StatusReport, error) {
	if len(f.Fields) < 3 {
		return nil, fmt.Errorf("h02: LINK requires 3+ fields, got %d", len(f.Fields))
	}
	timeStr := f.Fields[0]
	var dateStr string
	if len(f.Fields) >= 8 {
		dateStr = f.Fields[7]
	}
	ts, _ := parseDateTime(dateStr, timeStr)

	gsmSignal, _ := strconv.Atoi(f.Fields[1])
	satellites, _ := strconv.Atoi(f.Fields[2])

	return &StatusReport{
		IMEI:       f.IMEI,
		Timestamp:  ts,
		GSMSignal:  gsmSignal,
		Satellites: satellites,
	}, nil
}

// HTBTResponse builds the server response for a HTBT frame.
// Wire: *HQ,{IMEI},HS#\r\n
func HTBTResponse(imei string) []byte {
	return []byte("*HQ," + imei + ",HS#\r\n")
}

// SACKResponse builds the server response for a SACK frame.
// Wire: *SACK*HQ*{IMEI}*{serial}#\r\n
func SACKResponse(imei, serial string) []byte {
	return []byte("*SACK*HQ*" + imei + "*" + serial + "#\r\n")
}

// ── internal helpers ──────────────────────────────────────────────────────────

func parseDateTime(date, t string) (time.Time, error) {
	if len(date) < 6 || len(t) < 6 {
		return time.Time{}, fmt.Errorf("invalid date %q time %q", date, t)
	}
	day, _ := strconv.Atoi(date[0:2])
	month, _ := strconv.Atoi(date[2:4])
	yr2, _ := strconv.Atoi(date[4:6])
	year := yr2 + 2000
	if yr2 >= 70 {
		year = yr2 + 1900
	}
	hour, _ := strconv.Atoi(t[0:2])
	min, _ := strconv.Atoi(t[2:4])
	sec, _ := strconv.Atoi(t[4:6])
	if day == 0 || month == 0 || month > 12 {
		return time.Time{}, fmt.Errorf("invalid date day=%d month=%d", day, month)
	}
	return time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC), nil
}

func parseLatLon(latStr, ns, lonStr, ew string) (lat, lon float64, err error) {
	lat, err = nmeaToDecimal(latStr)
	if err != nil {
		return 0, 0, fmt.Errorf("lat %q: %w", latStr, err)
	}
	if strings.EqualFold(ns, "S") {
		lat = -lat
	}
	lon, err = nmeaToDecimal(lonStr)
	if err != nil {
		return 0, 0, fmt.Errorf("lon %q: %w", lonStr, err)
	}
	if strings.EqualFold(ew, "W") {
		lon = -lon
	}
	return lat, lon, nil
}

// nmeaToDecimal converts an NMEA DDDMM.MMMM coordinate string to decimal degrees.
// e.g. "3339.3525" → 33 + 39.3525/60 = 33.6559°
func nmeaToDecimal(s string) (float64, error) {
	dot := strings.Index(s, ".")
	if dot < 2 {
		return 0, fmt.Errorf("invalid NMEA coordinate %q (no decimal point or too short)", s)
	}
	degStr := s[:dot-2]
	minStr := s[dot-2:]
	deg, err := strconv.ParseFloat(degStr, 64)
	if err != nil {
		return 0, fmt.Errorf("degrees %q: %w", degStr, err)
	}
	min, err := strconv.ParseFloat(minStr, 64)
	if err != nil {
		return 0, fmt.Errorf("minutes %q: %w", minStr, err)
	}
	return deg + min/60.0, nil
}

// parseFlags extracts ACC-on and alarm flags from the 8-char hex flags string.
// Byte 0 (chars 0-1): basic status — bit 0 = ACC on
// Byte 2 (chars 4-5): alarm byte — bit 0 = SOS, bit 1 = power cut, bit 2 = vibration
func parseFlags(flags string) (accOn bool, alarmType uint8) {
	if len(flags) < 2 {
		return false, 0
	}
	// Byte 0: basic status
	if b0, err := strconv.ParseUint(flags[0:2], 16, 8); err == nil {
		accOn = uint8(b0)&Flag0ACC != 0
	}
	// Byte 2: alarm flags (if present)
	if len(flags) >= 6 {
		if b2, err := strconv.ParseUint(flags[4:6], 16, 8); err == nil {
			af := uint8(b2)
			if af&Flag2SOS != 0 {
				alarmType |= Flag2SOS
			}
			if af&Flag2PowerCut != 0 {
				alarmType |= Flag2PowerCut
			}
			if af&Flag2Vibration != 0 {
				alarmType |= Flag2Vibration
			}
		}
	}
	return accOn, alarmType
}

// Package protocol implements the H02/Sinotrack ASCII framing protocol.
//
// Wire format:
//
//	*HQ,{IMEI},{CMD},{field1},{field2},…#
//
// Frames are terminated by '#'. CRLF may follow on TCP streams.
// The IMEI is always the second comma-delimited field (index 1).
// The CMD is always the third field (index 2).
package protocol

import "time"

// Command names used in H02 ASCII frames.
const (
	CmdV1   = "V1"   // Standard GPS location report
	CmdV2   = "V2"   // Alternative GPS report (same field layout as V1)
	CmdHTBT = "HTBT" // Heartbeat / keep-alive
	CmdNBR  = "NBR"  // LBS-only location (no GPS fix)
	CmdLINK = "LINK" // Signal / status report
	CmdSACK = "SACK" // Device requests server ACK
)

// Status flag bits in byte 0 of the 4-byte little-endian hex flags field.
// Example flags field: "fbffff00" → byte[0]=0xfb, byte[1]=0xff, byte[2]=0xff, byte[3]=0x00
const (
	Flag0ACC       uint8 = 0x01 // ACC / ignition on
	Flag0Armed     uint8 = 0x02 // alarm armed
	Flag0Charging  uint8 = 0x04 // device charging
	Flag0GPS       uint8 = 0x08 // GPS tracking active
	Flag0Overspeed uint8 = 0x10 // overspeed threshold exceeded
	Flag0EnterFence uint8 = 0x20 // geo-fence entry
	Flag0ExitFence  uint8 = 0x40 // geo-fence exit
	Flag0DoorOpen   uint8 = 0x80 // door sensor open
)

// Flag2 bits in byte 2 of the flags field (alarm byte in common Sinotrack firmware).
const (
	Flag2SOS      uint8 = 0x01 // SOS alarm
	Flag2PowerCut uint8 = 0x02 // external power cut
	Flag2Vibration uint8 = 0x04 // vibration alarm
)

// Frame is a parsed H02 ASCII frame.
type Frame struct {
	IMEI   string
	Cmd    string
	Fields []string // fields after CMD (index 3 onwards)
	Raw    string   // original raw text including * and #
}

// LocationReport holds decoded GPS data from a V1, V2, or NBR frame.
type LocationReport struct {
	IMEI      string
	Timestamp time.Time
	Latitude  float64
	Longitude float64
	Speed     float64 // km/h (converted from knots)
	Course    float64 // degrees 0–359
	GPSFixed  bool
	ACCOn     bool
	AlarmType uint8  // normalised alarm flags (SOS, overspeed, etc.)
	RawFlags  string // original hex flags string (8 chars) for consumer interpretation
}

// StatusReport holds signal and status data from a LINK frame.
type StatusReport struct {
	IMEI       string
	Timestamp  time.Time
	GSMSignal  int
	Satellites int
}

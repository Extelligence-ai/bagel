"""Cast orangebox FieldDef to PyArrow Field."""

import pyarrow as pa
from orangebox import types

from src.topic import base

# This mapping is adapted from the `FRIENDLY_FIELD_NAMES` constant in the
# official Betaflight Blackbox Log Viewer. For more details, see:
# https://github.com/betaflight/blackbox-log-viewer
#
# Debug fields are not mapped because their interpretation is context-dependent
# and varies with the debug mode set in the recorded log. Contributions
# to map these dynamic fields are welcome.
FRIENDLY_FIELD_NAMES = {
    "axisP[all]": "PID P",
    "axisP[0]": "PID P [roll]",
    "axisP[1]": "PID P [pitch]",
    "axisP[2]": "PID P [yaw]",
    "axisI[all]": "PID I",
    "axisI[0]": "PID I [roll]",
    "axisI[1]": "PID I [pitch]",
    "axisI[2]": "PID I [yaw]",
    "axisD[all]": "PID D",
    "axisD[0]": "PID D [roll]",
    "axisD[1]": "PID D [pitch]",
    "axisD[2]": "PID D [yaw]",
    "axisF[all]": "PID Feedforward",
    "axisF[0]": "PID Feedforward [roll]",
    "axisF[1]": "PID Feedforward [pitch]",
    "axisF[2]": "PID Feedforward [yaw]",
    "axisS[all]": "PID S",
    "axisS[0]": "PID S [roll]",
    "axisS[1]": "PID S [pitch]",
    "axisS[2]": "PID S [yaw]",
    # Virtual field
    "axisSum[all]": "PID Sum",
    "axisSum[0]": "PID Sum [roll]",
    "axisSum[1]": "PID Sum [pitch]",
    "axisSum[2]": "PID Sum [yaw]",
    # Virtual field
    "axisError[all]": "PID Error",
    "axisError[0]": "PID Error [roll]",
    "axisError[1]": "PID Error [pitch]",
    "axisError[2]": "PID Error [yaw]",
    # Virtual field
    "rcCommands[all]": "Setpoints",
    "rcCommands[0]": "Setpoint [roll]",
    "rcCommands[1]": "Setpoint [pitch]",
    "rcCommands[2]": "Setpoint [yaw]",
    "rcCommands[3]": "Setpoint [throttle]",
    "rcCommand[all]": "RC Commands",
    "rcCommand[0]": "RC Command [roll]",
    "rcCommand[1]": "RC Command [pitch]",
    "rcCommand[2]": "RC Command [yaw]",
    "rcCommand[3]": "RC Command [throttle]",
    "gyroADC[all]": "Gyros",
    "gyroADC[0]": "Gyro [roll]",
    "gyroADC[1]": "Gyro [pitch]",
    "gyroADC[2]": "Gyro [yaw]",
    "gyroUnfilt[all]": "Unfiltered Gyros",
    "gyroUnfilt[0]": "Unfiltered Gyro [roll]",
    "gyroUnfilt[1]": "Unfiltered Gyro [pitch]",
    "gyroUnfilt[2]": "Unfiltered Gyro [yaw]",
    # End-users prefer 1-based indexing
    "motor[all]": "Motors",
    "motor[0]": "Motor [1]",
    "motor[1]": "Motor [2]",
    "motor[2]": "Motor [3]",
    "motor[3]": "Motor [4]",
    "motor[4]": "Motor [5]",
    "motor[5]": "Motor [6]",
    "motor[6]": "Motor [7]",
    "motor[7]": "Motor [8]",
    "eRPM[all]": "RPM",
    "eRPM[0]": "RPM [1]",
    "eRPM[1]": "RPM [2]",
    "eRPM[2]": "RPM [3]",
    "eRPM[3]": "RPM [4]",
    "eRPM[4]": "RPM [5]",
    "eRPM[5]": "RPM [6]",
    "eRPM[6]": "RPM [7]",
    "eRPM[7]": "RPM [8]",
    "servo[all]": "Servos",
    "servo[5]": "Servo Tail",
    "vbatLatest": "Battery volt.",
    "amperageLatest": "Amperage",
    "baroAlt": "Barometer",
    "heading[all]": "Heading",
    "heading[0]": "Heading [roll]",
    "heading[1]": "Heading [pitch]",
    "heading[2]": "Heading [yaw]",
    "accSmooth[all]": "Accel.",
    "accSmooth[0]": "Accel. [X]",
    "accSmooth[1]": "Accel. [Y]",
    "accSmooth[2]": "Accel. [Z]",
    "magADC[all]": "Compass",
    "magADC[0]": "Compass [X]",
    "magADC[1]": "Compass [Y]",
    "magADC[2]": "Compass [Z]",
    "flightModeFlags": "Flight Mode Flags",
    "stateFlags": "State Flags",
    "failsafePhase": "Failsafe Phase",
    "rxSignalReceived": "RX Signal Received",
    "rxFlightChannelsValid": "RX Flight Ch. Valid",
    "rssi": "RSSI",
    "GPS_numSat": "GPS Sat Count",
    "GPS_coord[0]": "GPS Latitude",
    "GPS_coord[1]": "GPS Longitude",
    "GPS_altitude": "GPS Altitude ASL",
    "GPS_speed": "GPS Speed",
    "GPS_ground_course": "GPS Heading",
    "gpsCartesianCoords[all]": "GPS Coords",
    "gpsCartesianCoords[0]": "GPS Coords [X]",
    "gpsCartesianCoords[1]": "GPS Coords [Y]",
    "gpsCartesianCoords[2]": "GPS Coords [Z]",
    "gpsDistance": "GPS Home distance",
    "gpsHomeAzimuth": "GPS Home azimuth",
    # Main frame time fields
    # https://betaflight.com/docs/development/Blackbox-Internals#log-frame-types
    "loopIteration": "Index of the current main loop iteration (starting at zero for the first logged iteration)",  # noqa: E501
    "time": "Timestamp of the beginning of the main loop in microseconds (this needn't start at zero, on Cleanflight it represents the system uptime).",  # noqa: E501
}


def to_pa_field(field_def: types.FieldDef) -> pa.Field:
    """Convert an orangebox FieldDef to a PyArrow Field.

    Args:
        field_def (types.FieldDef): The orangebox FieldDef to convert.

    Returns:
        pa.Field: The converted PyArrow Field.

    Notes:
        All fields are uniformly cast to the `pyarrow.int64()` type.

        The source flight controller blackbox format uses various C-style integer
        types (e.g., `int16_t`, `uint32_t`). For simplicity and to prevent potential
        data overflow, this function promotes all of these integer variations to a
        64-bit signed integer rather than mapping each one to a specific PyArrow
        type. See the exact integer types in the `blackboxMainState_s` struct:

        https://github.com/betaflight/betaflight/blob/master/src/main/blackbox/blackbox.c

    """
    description = FRIENDLY_FIELD_NAMES.get(field_def.name)
    return pa.field(
        name=field_def.name,
        type=pa.int64(),
        nullable=False,
        metadata={base.DESCRIPTION_KEY: description} if description else None,
    )

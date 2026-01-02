import textwrap

from src.topic.ros2 import definition
from src.topic.ros2.ros2msg import parse


def test_should_parse_sample_full_text() -> None:
    # GIVEN
    full_text = """
    # This message contains an uncompressed image
    # (0, 0) is at top-left corner of image

    std_msgs/Header header # Header timestamp should be acquisition time of image
                                # Header frame_id should be optical frame of camera
                                # origin of frame should be optical center of cameara
                                # +x should point to the right in the image
                                # +y should point down in the image
                                # +z should point into to plane of the image
                                # If the frame_id here and the frame_id of the CameraInfo
                                # message associated with the image conflict
                                # the behavior is undefined

    uint32 height                # image height, that is, number of rows
    uint32 width                 # image width, that is, number of columns

    # The legal values for encoding are in file include/sensor_msgs/image_encodings.hpp
    # If you want to standardize a new string format, join
    # ros-users@lists.ros.org and send an email proposing a new encoding.

    string encoding       # Encoding of pixels -- channel meaning, ordering, size
                        # taken from the list of strings in include/sensor_msgs/image_encodings.hpp

    uint8 is_bigendian    # is this data bigendian?
    uint32 step           # Full row length in bytes
    uint8[] data          # actual matrix data, size is (step * rows)

    ================================================================================
    MSG: std_msgs/Header
    # Standard metadata for higher-level stamped data types.
    # This is generally used to communicate timestamped data
    # in a particular coordinate frame.

    # Two-integer timestamp that is expressed as seconds and nanoseconds.
    builtin_interfaces/Time stamp

    # Transform frame with which this data is associated.
    string frame_id

    ================================================================================
    MSG: builtin_interfaces/Time
    # This message communicates ROS Time defined here:
    # https://design.ros2.org/articles/clock_and_time.html

    # The seconds component, valid over all int32 values.
    int32 sec

    # The nanoseconds component, valid in the range [0, 1e9).
    uint32 nanosec
    """

    # WHEN
    struct, deps = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.ComplexField(
            name="header",
            type_="std_msgs/Header",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            description="Header timestamp should be acquisition time of image",
        ),
        definition.UintField(
            name="height",
            type_="uint32",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description="image height, that is, number of rows",
        ),
        definition.UintField(
            name="width",
            type_="uint32",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description="image width, that is, number of columns",
        ),
        definition.StringField(
            name="encoding",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default="",
            description="Encoding of pixels -- channel meaning, ordering, size",
        ),
        definition.UintField(
            name="is_bigendian",
            type_="uint8",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description="is this data bigendian?",
        ),
        definition.UintField(
            name="step",
            type_="uint32",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description="Full row length in bytes",
        ),
        definition.UintField(
            name="data",
            type_="uint8",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description="actual matrix data, size is (step * rows)",
        ),
    ]
    assert deps["std_msgs/Header"].fields == [
        definition.ComplexField(
            name="stamp",
            type_="builtin_interfaces/Time",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            description=None,
        ),
        definition.StringField(
            name="frame_id",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default=None,
            description=None,
        ),
    ]


def test_should_parse_sample_full_text_with_builtin_types() -> None:
    # GIVEN
    full_text = """
    # This represents an estimate of a position and velocity in free space.
    # The pose in this message should be specified in the coordinate frame given by header.frame_id
    # The twist in this message should be specified in the coordinate frame given by the
    # child_frame_id

    # Includes the frame id of the pose parent.
    std_msgs/Header header

    # Frame id the pose points to. The twist is in this coordinate frame.
    string child_frame_id

    # Estimated pose that is typically relative to a fixed world frame.
    geometry_msgs/PoseWithCovariance pose

    # Estimated linear and angular velocity relative to child_frame_id.
    geometry_msgs/TwistWithCovariance twist

    ================================================================================
    MSG: geometry_msgs/msg/TwistWithCovariance
    # This expresses velocity in free space with uncertainty.

    Twist twist

    # Row-major representation of the 6x6 covariance matrix
    # The orientation parameters use a fixed-axis representation.
    # In order, the parameters are:
    # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
    float64[36] covariance

    ================================================================================
    MSG: geometry_msgs/msg/Twist
    # This expresses velocity in free space broken into its linear and angular parts.

    Vector3  linear
    Vector3  angular

    ================================================================================
    MSG: geometry_msgs/msg/Vector3
    # This represents a vector in free space.

    # This is semantically different than a point.
    # A vector is always anchored at the origin.
    # When a transform is applied to a vector, only the rotational component is applied.

    float64 x
    float64 y
    float64 z

    ================================================================================
    MSG: geometry_msgs/msg/PoseWithCovariance
    # This represents a pose in free space with uncertainty.

    Pose pose

    # Row-major representation of the 6x6 covariance matrix
    # The orientation parameters use a fixed-axis representation.
    # In order, the parameters are:
    # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
    float64[36] covariance

    ================================================================================
    MSG: geometry_msgs/msg/Pose
    # A representation of pose in free space, composed of position and orientation.

    Point position
    Quaternion orientation

    ================================================================================
    MSG: geometry_msgs/msg/Quaternion
    # This represents an orientation in free space in quaternion form.

    float64 x 0
    float64 y 0
    float64 z 0
    float64 w 1

    ================================================================================
    MSG: geometry_msgs/msg/Point
    # This contains the position of a point in free space
    float64 x
    float64 y
    float64 z

    ================================================================================
    MSG: std_msgs/msg/Header
    # Standard metadata for higher-level stamped data types.
    # This is generally used to communicate timestamped data
    # in a particular coordinate frame.

    # Two-integer timestamp that is expressed as seconds and nanoseconds.
    builtin_interfaces/Time stamp

    # Transform frame with which this data is associated.
    string frame_id

    ================================================================================
    MSG: builtin_interfaces/msg/Time
    # This message communicates ROS Time defined here:
    # https://design.ros2.org/articles/clock_and_time.html

    # The seconds component, valid over all int32 values.
    int32 sec

    # The nanoseconds component, valid in the range [0, 1e9).
    uint32 nanosec
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.ComplexField(
            name="header",
            type_="std_msgs/msg/Header",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            description=None,
        ),
        definition.StringField(
            name="child_frame_id",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.ComplexField(
            name="pose",
            type_="geometry_msgs/msg/PoseWithCovariance",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            description=None,
        ),
        definition.ComplexField(
            name="twist",
            type_="geometry_msgs/msg/TwistWithCovariance",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            description=None,
        ),
    ]

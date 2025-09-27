import textwrap

from src.topic.ros1 import definition, parse


def test_should_parse_sample_full_text() -> None:
    # GIVEN
    full_text = """
    # This represents an estimate of a position and velocity in free space.  
    # The pose in this message should be specified in the coordinate frame given by header.frame_id.
    # The twist in this message should be specified in the coordinate frame given by the child_frame_id
    Header header
    string child_frame_id
    geometry_msgs/PoseWithCovariance pose
    geometry_msgs/TwistWithCovariance twist

    ================================================================================
    MSG: std_msgs/Header
    # Standard metadata for higher-level stamped data types.
    # This is generally used to communicate timestamped data 
    # in a particular coordinate frame.
    # 
    # sequence ID: consecutively increasing ID 
    uint32 seq
    #Two-integer timestamp that is expressed as:
    # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
    # * stamp.nsec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
    # time-handling sugar is provided by the client library
    time stamp
    #Frame this data is associated with
    # 0: no frame
    # 1: global frame
    string frame_id

    ================================================================================
    MSG: geometry_msgs/PoseWithCovariance
    # This represents a pose in free space with uncertainty.

    Pose pose

    # Row-major representation of the 6x6 covariance matrix
    # The orientation parameters use a fixed-axis representation.
    # In order, the parameters are:
    # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
    float64[36] covariance

    ================================================================================
    MSG: geometry_msgs/Pose
    # A representation of pose in free space, composed of postion and orientation. 
    Point position
    Quaternion orientation

    ================================================================================
    MSG: geometry_msgs/Point
    # This contains the position of a point in free space
    float64 x
    float64 y
    float64 z

    ================================================================================
    MSG: geometry_msgs/Quaternion
    # This represents an orientation in free space in quaternion form.

    float64 x
    float64 y
    float64 z
    float64 w

    ================================================================================
    MSG: geometry_msgs/TwistWithCovariance
    # This expresses velocity in free space with uncertainty.

    Twist twist

    # Row-major representation of the 6x6 covariance matrix
    # The orientation parameters use a fixed-axis representation.
    # In order, the parameters are:
    # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
    float64[36] covariance

    ================================================================================
    MSG: geometry_msgs/Twist
    # This expresses velocity in free space broken into its linear and angular parts.
    Vector3  linear
    Vector3  angular

    ================================================================================
    MSG: geometry_msgs/Vector3
    # This represents a vector in free space. 
    # It is only meant to represent a direction. Therefore, it does not
    # make sense to apply a translation to it (e.g., when applying a 
    # generic rigid transformation to a Vector3, tf2 will only apply the
    # rotation). If you want your data to be translatable too, use the
    # geometry_msgs/Point message instead.

    float64 x
    float64 y
    float64 z
    """  # noqa: E501, W291

    # WHEN
    struct, deps = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.ComplexField(
            name="header",
            type_="std_msgs/Header",
            is_array=False,
            array_size=None,
            description=None,
        ),
        definition.BuiltInField(
            name="child_frame_id", type_="string", is_array=False, array_size=None, description=None
        ),
        definition.ComplexField(
            name="pose",
            type_="geometry_msgs/PoseWithCovariance",
            is_array=False,
            array_size=None,
            description=None,
        ),
        definition.ComplexField(
            name="twist",
            type_="geometry_msgs/TwistWithCovariance",
            is_array=False,
            array_size=None,
            description=None,
        ),
    ]
    assert deps["std_msgs/Header"].fields == [
        definition.BuiltInField(
            name="seq", type_="uint32", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="stamp", type_="time", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="frame_id", type_="string", is_array=False, array_size=None, description=None
        ),
    ]
    assert deps["geometry_msgs/PoseWithCovariance"].fields == [
        definition.ComplexField(
            name="pose",
            type_="geometry_msgs/Pose",
            is_array=False,
            array_size=None,
            description=None,
        ),
        definition.BuiltInField(
            name="covariance", type_="float64", is_array=True, array_size=36, description=None
        ),
    ]
    assert deps["geometry_msgs/Pose"].fields == [
        definition.ComplexField(
            name="position",
            type_="geometry_msgs/Point",
            is_array=False,
            array_size=None,
            description=None,
        ),
        definition.ComplexField(
            name="orientation",
            type_="geometry_msgs/Quaternion",
            is_array=False,
            array_size=None,
            description=None,
        ),
    ]
    assert deps["geometry_msgs/Point"].fields == [
        definition.BuiltInField(
            name="x", type_="float64", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="y", type_="float64", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="z", type_="float64", is_array=False, array_size=None, description=None
        ),
    ]
    assert deps["geometry_msgs/Quaternion"].fields == [
        definition.BuiltInField(
            name="x", type_="float64", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="y", type_="float64", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="z", type_="float64", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="w", type_="float64", is_array=False, array_size=None, description=None
        ),
    ]
    assert deps["geometry_msgs/TwistWithCovariance"].fields == [
        definition.ComplexField(
            name="twist",
            type_="geometry_msgs/Twist",
            is_array=False,
            array_size=None,
            description=None,
        ),
        definition.BuiltInField(
            name="covariance", type_="float64", is_array=True, array_size=36, description=None
        ),
    ]
    assert deps["geometry_msgs/Twist"].fields == [
        definition.ComplexField(
            name="linear",
            type_="geometry_msgs/Vector3",
            is_array=False,
            array_size=None,
            description=None,
        ),
        definition.ComplexField(
            name="angular",
            type_="geometry_msgs/Vector3",
            is_array=False,
            array_size=None,
            description=None,
        ),
    ]
    assert deps["geometry_msgs/Vector3"].fields == [
        definition.BuiltInField(
            name="x", type_="float64", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="y", type_="float64", is_array=False, array_size=None, description=None
        ),
        definition.BuiltInField(
            name="z", type_="float64", is_array=False, array_size=None, description=None
        ),
    ]

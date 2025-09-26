from src.source.px4.ulg import SourceFactory
from src.topic.px4.ulg import TopicRegistry


def test_topic_registry() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/px4/sample.ulg")
    data_source = factory.build()

    # WHEN
    registry = TopicRegistry(download_description=False)

    # THEN
    assert registry.available_topics(data_source) == [
        "actuator_armed_0",
        "actuator_controls_0_0",
        "actuator_outputs_0",
        "battery_status_0",
        "commander_state_0",
        "control_allocator_status_0",
        "cpuload_0",
        "estimator_attitude_0",
        "estimator_attitude_1",
        "estimator_event_flags_0",
        "estimator_event_flags_1",
        "estimator_innovation_test_ratios_0",
        "estimator_innovation_test_ratios_1",
        "estimator_innovation_variances_0",
        "estimator_innovation_variances_1",
        "estimator_innovations_0",
        "estimator_innovations_1",
        "estimator_local_position_0",
        "estimator_local_position_1",
        "estimator_selector_status_0",
        "estimator_sensor_bias_0",
        "estimator_sensor_bias_1",
        "estimator_states_0",
        "estimator_states_1",
        "estimator_status_0",
        "estimator_status_1",
        "estimator_status_flags_0",
        "estimator_status_flags_1",
        "estimator_visual_odometry_aligned_0",
        "estimator_visual_odometry_aligned_1",
        "estimator_visual_odometry_aligned_2",
        "estimator_visual_odometry_aligned_3",
        "event_0",
        "failure_detector_status_0",
        "home_position_0",
        "magnetometer_bias_estimate_0",
        "manual_control_setpoint_0",
        "mission_result_0",
        "offboard_control_mode_0",
        "position_setpoint_triplet_0",
        "px4io_status_0",
        "rate_ctrl_status_0",
        "rtl_time_estimate_0",
        "safety_0",
        "sensor_accel_0",
        "sensor_accel_1",
        "sensor_baro_0",
        "sensor_combined_0",
        "sensor_gyro_0",
        "sensor_gyro_1",
        "sensor_gyro_fft_0",
        "sensor_mag_0",
        "sensors_status_imu_0",
        "system_power_0",
        "takeoff_status_0",
        "telemetry_status_0",
        "telemetry_status_1",
        "trajectory_setpoint_0",
        "vehicle_acceleration_0",
        "vehicle_air_data_0",
        "vehicle_angular_velocity_0",
        "vehicle_attitude_0",
        "vehicle_attitude_setpoint_0",
        "vehicle_command_0",
        "vehicle_constraints_0",
        "vehicle_control_mode_0",
        "vehicle_imu_0",
        "vehicle_imu_1",
        "vehicle_imu_status_0",
        "vehicle_imu_status_1",
        "vehicle_land_detected_0",
        "vehicle_local_position_0",
        "vehicle_local_position_setpoint_0",
        "vehicle_magnetometer_0",
        "vehicle_rates_setpoint_0",
        "vehicle_status_0",
        "vehicle_status_flags_0",
    ]
    assert registry.native_type_name("vehicle_imu_0", data_source) == "vehicle_imu"
    assert registry.message_count("vehicle_imu_0", data_source) == 83
    # `registry.struct()` and `registry.describe()` are NOT tested here
    # because they require network access to download .msg files from
    # GitHub. This will make them very flaky in CI/CD environment.

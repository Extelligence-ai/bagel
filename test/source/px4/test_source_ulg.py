import pytest

from src.source.px4 import ulg


def test_source_factory() -> None:
    # GIVEN
    factory = ulg.SourceFactory("data/sample/px4/sample.ulg")

    # WHEN / THEN
    assert factory.total_message_count == 2018
    assert factory.start_seconds == 468.504411
    assert factory.end_seconds == 509.960381
    assert factory.msg_info_dict == {
        "ver_sw": "1c8ab2a0d7db2d14a6f320ebd8766b5ffaea28fa",
        "ver_sw_release": 17629952,
        "ver_hw": "PX4_FMU_V6C",
        "ver_hw_subtype": "V6C01",
        "sys_name": "PX4",
        "sys_os_name": "NuttX",
        "sys_os_ver": "4a1dd8680cd29f51fb0fe66dcfbf6f69bec747cf",
        "sys_os_ver_release": 184549631,
        "sys_toolchain": "GNU GCC",
        "sys_toolchain_ver": "9.3.1 20200408 (release)",
        "sys_mcu": "STM32H7[4|5]xxx, rev. V",
        "ver_data_format": 1,
        "sys_uuid": "000600000000313836333233510d00470048",
        "time_ref_utc": 0,
    }
    assert factory.changed_parameters == []
    assert factory.dropouts == []
    assert factory.has_data_appended is False
    assert factory.file_corruption is False
    assert factory.has_default_parameters is True


def test_validate_path_should_raise() -> None:
    # WHEN / THEN
    with pytest.raises(FileNotFoundError):
        ulg.SourceFactory("data/sample/px4/non_exist.ulg")

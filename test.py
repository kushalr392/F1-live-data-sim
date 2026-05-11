#!/usr/bin/env python3
"""
Auto-generated test suite by QA Agent.
Run with: pytest test.py -v
"""
import pytest
import sys
import os
import json
from unittest.mock import patch, MagicMock, ANY

# Mock environment variables before importing main to avoid initialization issues
@pytest.fixture(autouse=True, scope="session")
def mock_env_session():
    with patch.dict(os.environ, {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_TOPIC": "f1-telemetry",
        "KAFKA_DLQ_TOPIC": "f1-telemetry-dlq",
        "KAFKA_USERNAME": "test_user",
        "KAFKA_PASSWORD": "test_password",
        "DATA_INTERVAL": "0.1",
        "VEHICLE_ID": "TEST-F1-01"
    }):
        yield

# Patch KafkaProducer before importing main because main.py initializes it at module level
with patch('kafka.KafkaProducer') as MockProducer:
    # Add source directory to path so imports work
    sys.path.insert(0, os.path.dirname(__file__))
    import main

class TestTelemetryGeneration:
    def test_generate_telemetry_structure(self):
        """Tests that generate_telemetry returns a dictionary with all required keys."""
        telemetry = main.generate_telemetry()
        assert isinstance(telemetry, dict)
        expected_keys = [
            "vehicle_id", "speed", "lat", "lng", "g_force", "make",
            "rpm", "current_gear", "brake_temp", "tire_temp", "tire_psi", "timestamp"
        ]
        for key in expected_keys:
            assert key in telemetry

    def test_generate_telemetry_values(self):
        """Tests that generated telemetry values are within expected ranges and types."""
        telemetry = main.generate_telemetry()
        assert telemetry["vehicle_id"] == "TEST-F1-01"
        assert 100 <= telemetry["speed"] <= 350
        assert -90 <= telemetry["lat"] <= 90
        assert -180 <= telemetry["lng"] <= 180
        assert 0 <= telemetry["g_force"] <= 5
        assert 8000 <= telemetry["rpm"] <= 15000
        assert 1 <= telemetry["current_gear"] <= 8
        assert isinstance(telemetry["timestamp"], str)

    def test_generate_telemetry_randomness(self):
        """Tests that multiple calls generate different data."""
        t1 = main.generate_telemetry()
        t2 = main.generate_telemetry()
        # They should be different due to random values and timestamps
        assert t1 != t2

class TestKafkaCallbacks:
    @patch('main.logger')
    def test_on_send_success(self, mock_logger):
        """Tests the success callback logs correct info."""
        mock_metadata = MagicMock()
        mock_metadata.topic = "test-topic"
        mock_metadata.partition = 1
        mock_metadata.offset = 100
        
        main.on_send_success(mock_metadata)
        mock_logger.debug.assert_called()
        log_msg = mock_logger.debug.call_args[0][0]
        assert "test-topic" in log_msg
        assert "[1]" in log_msg
        assert "100" in log_msg

    @patch('main.produce_to_dlq')
    @patch('main.logger')
    def test_on_send_error_with_dlq(self, mock_logger, mock_dlq):
        """Tests the error callback logs error and calls DLQ."""
        exception = Exception("Kafka connection error")
        payload = b'{"data": "test"}'
        
        main.on_send_error(exception, payload)
        
        mock_logger.error.assert_called()
        assert "Kafka connection error" in str(mock_logger.error.call_args[0][0])
        mock_dlq.assert_called_once_with(payload, str(exception))

class TestDLQ:
    @patch('main.producer')
    @patch('main.logger')
    def test_produce_to_dlq_success(self, mock_producer, mock_logger):
        """Tests producing failed messages to DLQ."""
        value = b'{"speed": 300}'
        error_msg = "Test Error"
        
        main.produce_to_dlq(value, error_msg)
        
        # Verify producer.send was called
        mock_producer.send.assert_called_once()
        args, kwargs = mock_producer.send.call_args
        assert args[0] == "f1-telemetry-dlq"
        
        # Verify payload structure in DLQ
        dlq_payload = json.loads(kwargs['value'].decode('utf-8'))
        assert dlq_payload["original_payload"] == {"speed": 300}
        assert dlq_payload["error"] == "Test Error"
        assert "timestamp" in dlq_payload
        
        mock_logger.info.assert_called_with(f"Sent failed message to DLQ: f1-telemetry-dlq")

    @patch('main.producer')
    @patch('main.logger')
    def test_produce_to_dlq_exception(self, mock_producer, mock_logger):
        """Tests handling exceptions during DLQ production."""
        mock_producer.send.side_effect = Exception("DLQ Down")
        main.produce_to_dlq(b'{}', "Error")
        mock_logger.error.assert_called()
        assert "Failed to send to DLQ: DLQ Down" in str(mock_logger.error.call_args[0][0])

class TestMainExecution:
    @patch('main.producer')
    @patch('main.time.sleep', side_effect=KeyboardInterrupt)
    @patch('main.generate_telemetry')
    @patch('main.logger')
    def test_main_loop_and_cleanup(self, mock_logger, mock_gen, mock_sleep, mock_producer):
        """Tests that main generates data, sends it, and cleans up on KeyboardInterrupt."""
        mock_gen.return_value = {"vehicle_id": "TEST"}
        
        # main() has an infinite loop, so we mock sleep to raise KeyboardInterrupt to exit
        main.main()
        
        # Verify telemetry generation was called
        mock_gen.assert_called()
        
        # Verify producer.send was called
        mock_producer.send.assert_called()
        
        # Verify cleanup
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()
        mock_logger.info.assert_any_call("Simulator stopped by user.")

    @patch('main.producer')
    @patch('main.time.sleep')
    @patch('main.generate_telemetry')
    @patch('main.logger')
    def test_main_unexpected_error(self, mock_logger, mock_gen, mock_sleep, mock_producer):
        """Tests that main logs unexpected errors in the loop but continues or exits safely."""
        mock_gen.return_value = {"vehicle_id": "TEST"}
        # Raise an exception on first send, then KeyboardInterrupt on second sleep
        mock_producer.send.side_effect = Exception("Unexpected failure")
        mock_sleep.side_effect = KeyboardInterrupt
        
        main.main()
        
        mock_logger.error.assert_any_call("Unexpected error producing message: Unexpected failure")
        mock_producer.flush.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

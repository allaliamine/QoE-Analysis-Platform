import subprocess
import time
import sys

def run_consumer(consumer_file, consumer_name):
    print(f"Starting {consumer_name}...")
    
    command = [
        "spark-submit",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "--master", "spark://spark-master:7077",
        consumer_file
    ]
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"{consumer_name} started with PID: {process.pid}")
        return process
    except Exception as e:
        print(f"Error starting {consumer_name}: {e}")
        return None

def main():
    
    print("=" * 50)
    print("Starting QoE Analysis Spark Consumers")
    print("=" * 50)
    
    # Start gaming consumer
    gaming_process = run_consumer(
        "/opt/spark-apps/gaming_consumer.py",
        "Gaming QoE Consumer"
    )
    
    time.sleep(2)
    
    video_process = run_consumer(
        "/opt/spark-apps/video_consumer.py",
        "Video QoE Consumer"
    )
    
    print("\n" + "=" * 50)
    print("Both consumers are running!")
    print("Press Ctrl+C to stop all consumers")
    print("=" * 50 + "\n")
    
    try:
        
        while True:
            # Check if processes are still running
            if gaming_process and gaming_process.poll() is not None:
                print("Gaming consumer stopped unexpectedly")
                break
            if video_process and video_process.poll() is not None:
                print("Video consumer stopped unexpectedly")
                break
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\nStopping all consumers...")
        if gaming_process:
            gaming_process.terminate()
            print("Gaming consumer stopped")
        if video_process:
            video_process.terminate()
            print("Video consumer stopped")
        print("All consumers terminated successfully")
        sys.exit(0)

if __name__ == "__main__":
    main()

# Makefile — Thành viên A quick commands
# Chạy với: make <target>

.PHONY: install setup run-task1 run-task2 run-task3 run-all clean

## Task 1: Cài đặt môi trường
install:
	pip install -r requirements.txt

## Task 1.3: Tạo bảng Delta Lake mẫu
setup:
	python setup_environment.py

## Task 2: Chạy workload generator (tất cả 3 patterns)
run-task2:
	python -c "import sys; sys.path.insert(0, '.'); from src.workload_generator import WorkloadGenerator; [WorkloadGenerator(num_writers=4, num_rows_per_writer=20, pattern_type=p).run() for p in ['A','B','C']]"

## Task 3: Chạy conflict strategy benchmark
run-task3:
	python -c "import sys; sys.path.insert(0, '.'); from src.conflict_strategy import BenchmarkRunner; BenchmarkRunner(num_writers=4, conflict_ratio=1.0, pattern='B').run_all()"

## Chạy tất cả Tasks (1+2+3)
run-all: setup run-task2 run-task3

## Xóa data và results để chạy lại từ đầu
clean:
	if exist data\transactions rmdir /s /q data\transactions
	if exist results rmdir /s /q results
	mkdir results

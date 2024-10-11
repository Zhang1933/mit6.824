#!/bin/bash

# 初始化计数器
count=0
success_count=0
fail_count=0

# 设置测试次数
max_tests=100
threads=$max_tests  # 设置线程数
testcase=2D
testtime=2

# 定义一个函数来运行单个测试
run_test() {
    local i=$1
    echo "Running test $testcase $testtime iteration $i of $max_tests..."

    # 运行 go 测试命令
    go test -v -run $testcase &> output$testcase-$testtime-$i.log
    
    # 检查 go 命令的退出状态
    if [ "$?" -eq 0 ]; then
        # 测试成功
        echo "Test iteration $i passed."
        success_count=$((success_count+1))
        # 如果想保存通过的测试日志，取消下面行的注释
        # mv output2C_$i.log "success_$i.log"
    else
        # 测试失败
        echo "Test iteration $i failed, check 'failure$testcase-$testtime-$i.log' for details."
        mv output$testcase-$testtime-$i.log "failure$testcase-$testtime-$i.log"
        fail_count=$((fail_count+1))
    fi
}

# 使用多线程运行测试
for ((i=1; i<=max_tests; i++))
do
    # 启动线程执行测试
    run_test "$i" &
    
    # 如果当前运行的线程数达到了设置的最大线程数，等待线程完成
    if (( i % threads == 0 )); then
        wait
    fi
done

# 等待所有后台线程完成
wait

# 报告测试结果
echo "Testing completed: $max_tests iterations run."
echo "Successes: $success_count"
echo "Failures: $fail_count"



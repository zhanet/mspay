#!/usr/bin/env php
<?php
/**
 * 民生数据下发与结果检查
 */

if (!function_exists('pcntl_fork')) {
    die("pcntl_fork not existing");
}

// 参数检查与命令用法
if (count($argv)!=2 || ($argv[1] != 'check' && $argv[1] !='push') ) {
    die("{$argv[0]} push  - push withdraw data\n{$argv[0]} check - check push results\n");
}

$cmd = 'php '.(dirname(__FILE__))."/../index.php mspay rec_count {$argv[1]}";
//passthru($cmd);
$out = popen($cmd, "r");
$count = fread($out, 512); //待处理记录数
pclose($out);
//echo $count;exit;
$p_size = 10; // 单进程处理数
$time_over = 60; // 超时(秒数)
$workers = ceil(trim($count) / $p_size);
$workers = $workers <=10 ? $workers : 5; // 最大进程数
$cmd = 'php '.(dirname(__FILE__))."/../index.php mspay ";
if ($workers == 0) exit; //没有数据退出

// 创建管道(进程间通信)
$pipe_file = "pipe.".posix_getpid();
if (!posix_mkfifo($pipe_file, 0666)) {
    die("create pipe {$pipe_file} error");
}

for ($i = 0; $i < $workers; ++$i ) {
    $pid = pcntl_fork(); // 创建子进程
    if ($pid == 0) {
        //sleep(rand(1,3)); // 模拟任务执行
        if ($argv[1] == 'check') {
            $cmd .= "auto_withdraw {$i} {$p_size}";
        } else if ($argv[1] == 'push') {
            $cmd .= "withdraw {$i} {$p_size} 50000";
        }
        passthru($cmd);

        $pipe = fopen($pipe_file, 'w');
        fwrite($pipe, $i."\n");
        fclose($pipe);
        exit(0);
    }
}

// 父进程
$pipe = fopen($pipe_file, 'r');
stream_set_blocking($pipe, FALSE); // 设置为非堵塞，适应超时机制
$pipe_data = '';
$pipe_line = 0;
$start_time = time();
while ($pipe_line < $workers && (time() - $start_time) < $time_over) {
    $line = fread($pipe, 1024);
    if (empty($line)) {
        continue;
    }

    echo 'worker '.$line."\n";
    // 分析多少任务处理完毕，通过‘\n’标识
    foreach(str_split($line) as $c) {
        if ("\n" == $c) {
            ++$pipe_line;
        }
    }
    $pipe_data .= $line;
}
fclose($pipe);
unlink($pipe_file);
echo "worker count: $pipe_line\n";

// 等待子进程执行完毕，避免僵尸进程
$n = 0;
while ($n < $workers) {
    $status = -1;
    $pid = pcntl_wait($status, WNOHANG);
    if ($pid > 0) {
        echo "{$pid} exit\n";
        ++$n;
    }
}

// 任务完成情况
$task = array();
foreach(explode("\n", $pipe_data) as $i) {
    if (is_numeric(trim($i))) {
        array_push($task, $i);
    }
}
$task = array_unique($task);
if ( count($task) == $workers ) {
    echo 'ok'."\n";
} else {
    echo 'timeout count '.count($task)."\n";
    //var_dump($task);
}

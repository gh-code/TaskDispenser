<?php
/**
 * Tasks shared amoung hosts/processes automatically
 *
 * Author: Gary Huang <gh.nctu+code@gmail.com>
 */

class TimeoutException extends Exception { }
class IOException extends Exception { }
class BadInitialException extends Exception { }

class stopwatch {

    public function __construct($start=true) {
        if ($start) {
            $this->start();
        }
    }

    public function start() {
        $this->start = microtime(true);
    }

    public function stop() {
        return microtime(true) - $this->start;
    }
}

abstract class mutex {
    abstract public function lock();
    abstract public function try_lock();
    abstract public function unlock();
}

class file_mutex extends mutex {

    private $handle;

    public function __construct($filename) {
        $handle = fopen($filename, 'w');
        if ($handle === false) {
            throw new IOException('fail to create barrier');
        }
        $this->handle = $handle;
    }

    public function __destruct() {
        fclose($this->handle);
    }

    public function lock() {
        flock($this->handle, LOCK_EX);
    }

    public function try_lock() {
        return flock($this->handle, LOCK_EX | LOCK_NB);
    }

    public function unlock() {
        flock($this->handle, LOCK_UN);
    }
}

class condition_variable {

    private $period;
    private $mutex;

    public function __construct() {
        $this->period = 0.1;
        $this->mutex = null;
    }

    public function set_poll_period($period) {
        $this->period = $period;
    }

    public function notify_all() {
        if ($this->mutex) {
            $this->mutex->unlock();
        }
    }

    public function wait(mutex $mutex) {
        $this->mutex = $mutex;
        $this->mutex->lock();
    }

    public function wait_until(mutex $mutex, $timeout) {
        $this->mutex = $mutex;
        $count = 0;
        $period = intval($this->period * 1e6);
        $base = intval(1e6 / $period);
        while (!$this->mutex->try_lock()) {
            usleep($period);
            $time = $count / $base;
            if ($time > $timeout) {
                throw new TimeoutException();
            }
            $count++;
        }
        return $count;
    }
}

function barrier($filename, $timeout=0, $callback=null, $args=null, $period=0.1) {

    $m = new file_mutex($filename);
    $cv = new condition_variable;
    $cv->set_poll_period($period);
    $waited = $cv->wait_until($m, $timeout);

    // main process
    if (!$waited) {
        $stopwatch = new stopwatch;
        if ($callback) {
            if ($args) {
                $callback($args);
            } else {
                $callback();
            }
        }
        $elapsed = $stopwatch->stop();
        $timeout = intval($timeout * 1e6 - ceil($elapsed * 1e3) * 1e3);
        usleep($timeout);
    }

    $cv->notify_all();
}

class runtime_report {

    private $stopwatch;

    public function __construct(stopwatch $stopwatch) {
        $this->stopwatch = $stopwatch;
    }

    public function __destruct() {
        $this->stop();
    }

    public function stop() {
        $elapsed = round($this->stopwatch->stop(), 6);
        echo "runtime: {$elapsed}s\n";
    }
}

function register_process($filename, $data) {

    if (!file_exists($filename)) {
        touch($filename);
    }

    $fp = fopen($filename, 'a');

    if ($fp === false) {
        throw new IOException('fail to open file: ' . $filename);
    }

    flock($fp, LOCK_EX);
    fputs($fp, $data . "\n");

    fclose($fp);
}

function count_processes($filename) {

    $fp = fopen($filename, 'r');

    if ($fp === false) {
        throw new IOException('fail to open file: ' . $filename);
    }

    flock($fp, LOCK_SH);
    $count = 0;
    while (fgets($fp) !== false) {
        $count++;
    }

    fclose($fp);

    return ($count > 0 ? $count : 1);
}

function is_flocked($filename) {
    if (!file_exists($filename)) {
        return false;
    }
    $fp = fopen($filename, 'r');
    if ($fp === false) {
        return true;
    }
    $result = !flock($fp, LOCK_EX | LOCK_NB);
    fclose($fp);
    return $result;
}

function count_tasks() {
    return 10;
}

function create_tasks($args) {

    global $debug_mode;

    $filename = $args[0];
    $tasks = $args[1];
    $processfile = $args[2];
    $barrier2 = $args[3];
    $lockfile = isset($args[4]) ? $args[4] : 'share.lock';

    if (is_flocked($processfile) ||
        is_flocked($lockfile) ||
        is_flocked($barrier2)) {
        throw new BadInitialException('bad initial state: ' . $filename);
    }

    if (file_exists($filename)) {
        $timediff = time() - filemtime($filename);
        if ($debug_mode < 1 && $timediff < 30) {
            throw new IOException('file is just used for ' . $timediff . 's: ' . $filename);
        }
    }

    $fp = fopen($filename, 'w');

    if ($fp === false) {
        throw new IOException('fail to open file: ' . $filename);
    }

    foreach ($tasks as $task) {
        fputs($fp, "{$task}\n");
    }

    fclose($fp);
}

function share_tasks($filename, $num=-1, $lockfile='share.lock') {

    $tempfile = $filename . '.tmp';
    $fp_lock = fopen($lockfile, 'w');
    $fp_temp = fopen($tempfile, 'w');
    $fp = fopen($filename, 'r');

    if ($fp_lock === false) {
        throw new IOException('fail to open file: ' . $lockfile);
    }
    if ($fp_temp === false) {
        throw new IOException('fail to open file: ' . $tempfile);
    }
    if ($fp === false) {
        throw new IOException('fail to open file: ' . $filename);
    }

    flock($fp_lock, LOCK_EX);

    $results = array();
    while (($line = fgets($fp)) !== false) {
        if ($num > 0) {
            $results[] = trim($line);
            $num--;
        } else if ($num < 0) {
            $results[] = trim($line);
        } else {
            fputs($fp_temp, $line);
        }
    }

    fclose($fp);
    fclose($fp_temp);

    rename($tempfile, $filename);

    // flock($fp_lock, LOCK_UN);
    fclose($fp_lock);

    return $results;
}

function auto_share_tasks($tasks) {

    global $debug_mode;

    $taskfile = 'task.txt';
    $processfile = 'process.txt';
    $barrier1 = 'barrier1.lock';
    $barrier2 = 'barrier2.lock';
    $barrier1_wait = 0.5;
    $barrier2_wait = 0.1;
    $shared_tasks = array();

    try {

        $pid = getmypid();
        $hostname = gethostname();
        if ($debug_mode > 0) {
            echo "hostname: {$hostname}\n";
            echo "process id: {$pid}\n";
        }
        register_process($processfile, $pid);

        try {
            barrier($barrier1, $barrier1_wait, 'create_tasks',
                array($taskfile, $tasks, $processfile, $barrier2));
        } catch (BadInitialException $e) {
            if ($debug_mode > 0) {
                echo "miss first barrier\n";
            }
        }

        $num_processes = count_processes($processfile);
        if ($debug_mode > 0) {
            echo "#processes: {$num_processes}\n";
        }
        $num = intval(count($tasks) / $num_processes);

        $shared_tasks = share_tasks($taskfile, $num);
        barrier($barrier2, $barrier2_wait);

        $shared_tasks = array_merge($shared_tasks, share_tasks($taskfile));

    } catch (TimeoutException $e) {
    } catch (IOException $e) {
        echo $e->getMessage() . "\n";
    } catch (Exception $e) {
        echo $e->getMessage() . "\n";
    }

    if (file_exists($processfile)) {
        unlink($processfile);
    }

    return $shared_tasks;
}

function generate_random_tasks($num) {
    $tasks = array();
    for ($i = 1; $i <= $num; ++$i) {
        $tasks[] = "task{$i}";
    }
    shuffle($tasks);
    return $tasks;
}

function consume_tasks(&$tasks, $num_threads=1) {
    foreach ($tasks as $task) {
        echo $task . "\n";
    }
    echo "done\n";
}

$debug_mode = 1;
$num_threads = 4;

$reporter = new runtime_report(new stopwatch);

$tasks = generate_random_tasks(10);
$shared_tasks = auto_share_tasks($tasks);
consume_tasks($shared_tasks, $num_threads);

?>

<?php

/**
 * 提现申请数据下发 - 民生通道(宝易互通)
 */

class mspay extends Cli_Controller
{

    private $log_file = 'mspay.log';
    private $t_amount = 50000; //银行单笔提现限额
    private $amount_limit = 99999999; //单笔提现最大限额
    private $ban_users = array(); //临时禁止提现名单
    private $t1,$t2;

    /**
     * 启动时执行
     */
    function _init() {
        set_time_limit(0);
        $this->t1 = datetime();
        $this->t2 = microtime(true);
        // 关闭查询记录，防止内存不足，脚本挂掉
        $this->db->save_queries = false;
        // 涉及另外一个数据库时需手动关闭
        $this->User->db->save_queries = false;
        // 临时禁止提现名单
        $this->ban_users = $this->Setting->get_item('auto_withdraw_ban_users');
        $this->ban_users = $this->ban_users ? explode(',', $this->ban_users) : array();
        //$this->log = 'ms.'.$this->uri->rsegment(1).".".$this->uri->rsegment(2);
    }

    /**
     * 关闭时执行
     */
    function __destruct() {
        $this->tolog($this->log_file, "".$this->t1.'  '.round(microtime(true) - $this->t2, 1)."\n");
    }

    /**
     * 用民生宝易互通的单笔实名代付功能下发提现数据
     *
     * @param int $pid
     * @param int $p_size
     * @param null $amount_limit
     * @param bool|FALSE $force_run
     * @param bool|FALSE $ignore_error
     * @return bool
     */
    function withdraw($pid=0, $p_size=1, $amount_limit=NULL, $force_run=FALSE, $ignore_error=FALSE)
    {
        //单笔提现限额
        if($amount_limit == NULL) $amount_limit = $this->amount_limit;

        //非强制执行检查状态
        if($force_run == FALSE){
            $is_auto = $this->Setting->get_item('auto_withdraw');
            if(!$is_auto) return FALSE; //检查自动提现状态, 若处于关闭状态则返回
        }
        //$this->tolog($this->log_file, "{$pid}: ".datetime()." CP0003\n");

        //按分页取提现订单, 跳过出错记录
        $sql = "SELECT * FROM plan_withdraw WHERE status=0 AND auto=0 AND auto_group=1 AND error='' AND order_id='' AND amount<='{$amount_limit}' AND id>=";
        $sql .= "(SELECT id FROM plan_withdraw WHERE status=0 AND auto=0 AND auto_group=1 AND error='' AND order_id='' AND amount<='{$amount_limit}' ";
        $sql .= "ORDER BY id LIMIT ".$pid * $p_size.",1) ";
        $sql .= "ORDER BY id LIMIT {$p_size}";

        $list = $arr = array();
        $arr = $this->db->query($sql)->result_array();
        foreach($arr as $v) {
            //提现信息预审, 通过后加入提现列表
            $this->withdraw_info($v, $list, $ignore_error);
        }

        //print_r($list);
        //提现下发并返回成功条数
        $n = $this->process($list);
        $this->tolog($this->log_file, "{$pid}: apply {$n}\n");
        return count($list) == $n;
    }

    /**
     * 返回可处理记录数
     * @param string $action
     */
    function rec_count($action='push')
    {
        if ($action == 'push') {
            $sql = "SELECT COUNT(id) c FROM plan_withdraw WHERE status=0 AND auto=0 AND auto_group=1 AND error='' AND order_id=''";
        } else if ($action == 'check') {
            $sql = "SELECT COUNT(id) c FROM plan_withdraw WHERE auto=1 AND auto_group=1 AND status IN(1) AND order_id<>'' AND TIMESTAMPDIFF(MINUTE,time,now())>30";
        } else return;
        $r = $this->db->query($sql)->result_array();
        echo $r[0]['c'].' ';
    }

    /**
     * 提现预审
     * @param $v
     * @param $list
     * @param $ignore_error
     * @return array|bool
     */
    function withdraw_info(&$v, &$list, $ignore_error)
    {
        // 提现预审, 并取账户相关信息
        $pre_check = $this->Plan_withdraw->pre_check($v);
        if(!$ignore_error && !$pre_check) return FALSE;

        // 被禁止提现名单内的用户不予处理
        if(in_array($v['name'], $this->ban_users)) return FALSE;

        // 套现标记用户两天后再给予提现
        $is_ccc = $this->User->get_one($v['owner'], 'is_ccc');
        if($is_ccc && (time()-strtotime($v['time']) < 86400*2) ) return FALSE;

        // 套现嫌疑标记用户一天后再给予提现
        $is_cccs = $this->User->get_one($v['owner'], 'is_cccs');
        if($is_cccs && (time()-strtotime($v['time']) < 86400*1) ) return FALSE;

        // 去掉可能存在的空格
        $v['bank']['bank_province'] = preg_replace('/\s+/', '', $v['bank']['bank_province']);
        $v['bank']['bank_name'] = preg_replace('/\s+/', '', $v['bank']['bank_name']);
        $bank_name = $this->bank_name($v['bank']['bank']); //规范银行名称
        $fee = $this->Plan_withdraw->fee($v['owner']); //提现手续费
        $item = array(
            "companyTransferId" => $v['id'],
            "transferMoney" => round($v['amount']-$fee, 2),
            "accName" => $v['bank']['realname'],
            "accNo" => $v['bank']['account'],
            "bankName" => $bank_name,
            "proName" => $v['bank']['bank_province'],
            "cityName" => $v['bank']['bank_city'],
            //"accDept" => $v['bank']['bank_name'],
            "idNo" => $v['idnum'],
        );
        //单条下发接口, 大额提现暂不处理
        if ( $item['transferMoney'] <= $this->t_amount ) {
            //通过预审, 则锁定为自动处理状态, 防止CP0003卡住时被其它进程获取
            //后续处理: 若 status=0 AND auto=1 表示未被CP0003处理, 可重发
            $this->Plan_withdraw->set($v['id'], array('auto'=>1));
            $list[] = $item;
        }
        return true;
    }

    /**
     * 民生提现数据下发
     * @param $list
     * @return int
     */
    function process(&$list)
    {
        $n = 0;
        if ( ! $list) return 0;
        $this->load->library("Mspay_fast"); //民生数据接口

        foreach ($list as $arr)
        {
            $item = array(
                'realname'=> $arr['accName'],
                'id_card' => $arr['idNo'],
                'account' => $arr['accNo'],
                'amount' => $arr['transferMoney'],
                'accountType' => '00', //00对私 01对公
                'bank_province' => $arr['proName'],
                'bank_city' => $arr['cityName'],
                //'bank_name' => $arr['accDept'],
                'bank' => $arr['bankName'], // 所属银行
                //'certType' => '',
            );

            $id = $arr['companyTransferId']; //提现订单号
            $r = $this->mspay_fast->CP0003($item, $res, $order_id);

            if ($res['head']['respcode'] == 'C000000000') {
                //交易成功
                switch($r) {
                    case '00': //代付受理成功,之后需CP0004确认
                        $this->Plan_withdraw->set($id, array('status'=>1,'order_id'=>$order_id));
                        break;
                    case '01': //实时代付成功
                        $this->Plan_withdraw->set($id, array('status'=>2,'order_id'=>$order_id));
                        $this->tolog($this->log_file, "{$id} {$order_id}\n");
                        $this->action($id); //执行资金操作
                        print_r($res);
                        break;
                    case '02': //处理中,之后CP0004确认,但结果可能查不到?
                        $this->Plan_withdraw->set($id, array('status'=>3,'order_id'=>$order_id));
                        break;
                    case '03': //代付失败
                        $this->Plan_withdraw->set($id, array('status'=>-2,'error'=>$res['head']['respmsg']));
                        break;
                }
                $n++;
            } else if ($res['head']['respcode'] == 'W000000000') {
                //处理中(由于网络原因,不知道结果,之后需要CP0004查询确认,记下流水号)
                $this->Plan_withdraw->set($id, array('status'=>1,'order_id'=>$res['head']['bussflowno']));
            } else {
                //失败
                //print_r($item);
                //$err_msg = print_r($res, TRUE);
                $err_msg = isset($res['head']['respmsg']) ? $res['head']['respmsg'] : 'err';
                //如果遇到余额不足错误，关闭自动下发并返回
                if(strstr($err_msg, '余额不足') || strstr($err_msg, '余额小于0')){
                    $this->Setting->set_item('auto_withdraw', 0);
                    return $n;
                }
                $err_msg = $order_id ? "{$order_id} {$err_msg}" : $err_msg;
                $this->Plan_withdraw->set($id, array('status'=>-2,'error'=>$err_msg));
                $this->tolog($this->log_file, "{$id} {$err_msg}\n");
            }
            usleep(100);
        }
        return $n;
    }

    /**
     * 检查之前提现数据下发结果, 并进行相应的处理
     * @param int $pid
     * @param int $p_size
     * @return bool
     */
    function auto_withdraw($pid=0, $p_size=1)
    {
        //$this->tolog($this->log_file, "{$pid}: ".datetime()." CP0004\n");
        $this->load->library("Mspay_fast"); //民生数据接口
        $m = $n = 0;

        //取出已经下发处于自动处理状态的订单
        $sql = "SELECT * FROM plan_withdraw WHERE auto=1 AND auto_group=1 AND status IN(1) AND order_id<>'' AND TIMESTAMPDIFF(MINUTE,time,now())>30 AND id>=";
        $sql .= "(SELECT id FROM plan_withdraw WHERE auto=1 AND auto_group=1 AND status IN(1) AND order_id<>'' AND TIMESTAMPDIFF(MINUTE,time,now())>30 ";
        $sql .= "ORDER BY id LIMIT ".$pid * $p_size.",1) ";
        $sql .= "ORDER BY id LIMIT {$p_size}";

        $arr = $this->db->query($sql)->result_array();
        foreach ($arr as $v) {
            $status = $this->mspay_fast->CP0004($v['order_id'], $res);
            switch ($status) {
                case '00' : // 代付受理成功(非实时代付交易返回)
                    //短信通知: ? 您的提现申请已经受理，具体到账时间请以银行通知为准（或开通到账通知）【米袋计划】
                    break;
                case '01' : // 代付成功(实时代付交易返回)
                    $this->Plan_withdraw->set($v['id'], array('status'=>2));
                    $this->action($v['id']);
                    $m++;
                    break;
                case '02' : // 系统处理中
                    break;
                case '03' : // 代付失败
                    $this->Plan_withdraw->set($v['id'], array('status'=>-2, 'error'=> $res['body']['tranRespMsg']));
                    //短信通知: ? 您的提现申请失败，原因可能是:...【米袋计划】
                    $n++;
                    break;
            }
            usleep(100); //间隔100毫秒
        }

        $x = count($arr);
        $this->tolog($this->log_file, "{$pid}: query {$x}, update {$m} {$n}\n");
        return true;
    }

    /**
     * 执行提现资金操作
     * @param $id
     * @return bool
     */
    function action($id) {
        // 判断用户锁定金额是否足够
        $line = $this->Plan_withdraw->get_line($id);
        $stat = $this->Plan_account->get_stat('user', $line['owner']);
        if ($stat['locking'] >= $line['amount']) {
            //提现资金操作
            $fee = $this->Plan_withdraw->fee($line['owner']); //计算提现手续费
            $r = $this->Plan_account->op('withdraw', sprintf("%.4f", $line['amount']-$fee), 'user:'.$line['owner'], 'user_bank:'.$line['bank_id']);
            //扣除手续费
            if ($r && $fee) {
                $this->Plan_account->op('withdraw_fee', $fee, 'user:'.$line['owner'], 'withdraw_fee') &&
                $this->Plan_withdraw->set($line['id'], array('fee'=>$fee));
            }
            //发送短信提醒
            if($r){
                $this->load->library('Sms');
                $phone = $this->User->get_one($line['owner'], 'phone');
                if($phone){
                    $content = "您的提现".sprintf("%.4f", $line['amount']-$fee)."元已经到账，请注意查收。欢迎登录米袋官网或在手机app继续投资理财。【米袋计划】";
                    //$this->tolog($this->log_file, "{$content}\n");
                    $this->sms->send($phone, $content);
                    return true;
                }
            }
        } else {
            $this->Plan_withdraw->set($id, array('status'=>-2, 'error'=>'锁定金额不够'));
            $this->tolog($this->log_file, "{$id} 锁定金额不够\n");
            return false;
        }
    }

    /**
     * 银行名称规范化
     * @param $name
     * @return mixed
     */
    function bank_name($name) {
        $bank_name = array(
            '民生银行'=>'中国民生银行',
            '广发银行'=>'广东发展银行',
            '工商银行'=>'中国工商银行',
            '农业银行'=>'中国农业银行',
            '建设银行'=>'中国建设银行',
            '光大银行'=>'中国光大银行',
            '浦发银行'=>'上海浦东发展银行',
            '浦东发展银行'=>'上海浦东发展银行',
            '邮政储蓄银行'=>'中国邮政储蓄银行',
        );
        if (array_key_exists($name, $bank_name)) {
            return $bank_name[$name];
        } else {
            return $name;
        }
    }

}

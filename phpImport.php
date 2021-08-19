<?php

$config = [];
if (file_exists("config.ini"))
$config = parse_ini_file("config.ini");

function conf($e){
    global $config;

    if (isset($config[$e]))
        return $config[$e];
    else
        return false;
}


if (!isset($_SERVER['DOCUMENT_ROOT']) | $_SERVER['DOCUMENT_ROOT']==''){
    $_SERVER['DOCUMENT_ROOT'] = conf('DOCUMENT_ROOT');
}

define('TLG_ADM_ID', conf('tlg_user'));  
define('TLG_BOT_TOKEN', conf('tlg_bot'));


function tlg($messaggio){
    if (gettype($messaggio)=='array') $messaggio = print_r($messaggio, true);
    $messaggio .= "\n\n ";

    $url = "https://api.telegram.org/bot" . TLG_BOT_TOKEN . "/sendMessage?chat_id=" . TLG_ADM_ID;
    $url = $url . "&text=" . urlencode($messaggio);
    $ch = curl_init();
    $optArray = array(
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true
    );
    curl_setopt_array($ch, $optArray);
    $result = curl_exec($ch);
    curl_close($ch);
    return $result;
}


function folderContent($dir)
{
    $filelist = array("folder" => array(), "file" => array());

    foreach (scandir($dir) as $value) {
        if (is_dir(rtrim($dir, "/") . "/" . $value))
            $filelist["folder"][] = $value; //trim($value, ".");
        else
            $filelist["file"][] = $value; //trim($value, ".");
    }

    return $filelist;
}


class phpImport {
    private $query;
    private $queryArray;
    private $index = 0;
    private $handle;
    private $filename;
    private $bfr = '';
    private $ftell;
    private $eof = false;
    private $parseValues = false;
    private $seek = 0;
    public  $queris = [];
    private $maxInsert = 500;
    private $dieAfterCountBugs = 30;
    private $config = [];
    private $baseDir = '';

    private $needSaveConfig = 10;

    private $counts = 0; 

    private $mysqli;

    private $setSET = false;
    private $createTable = false;
    private $bugsInsert = [];

    function __construct($filename, $empty = false){
        $this->seek = 0;
        $connection = [
            "host" => conf('mysql_host'),
            "user" => conf('mysql_user'),
            "password" => conf('mysql_password'),
            "database" => conf('mysql_database')
        ];

        $this->mysqli = new mysqli($connection['host'], $connection['user'], $connection['password']);
        mysqli_select_db($this->mysqli, $connection['database']);


        $this->filename = $filename;


        $this->baseDir = conf('savePath');

        if (file_exists($this->baseDir . '/session.json')){
            if (!$empty) $config = json_decode(file_get_contents($this->baseDir . '/session.json'), true);
            
            if (isset($config[$this->filename])){
                $this->config = $config[$this->filename];
            }
        }

        if (isset($this->config['seek'])) $this->seek = $this->config['seek'];
        if (isset($this->config['counts'])) $this->counts = $this->config['counts'];
        if (isset($this->config['SET'])) $this->queris['SET'] = $this->config['SET'];
    }

    function __destruct(){
        $this->saveSession();

    }

    function saveSession(){
        $this->config['seek'] = $this->seek;
        $this->config['counts'] = $this->counts;

        if (isset($this->queris['SET']))
            $this->config['SET'] = $this->queris['SET'];



        $file_name = $this->baseDir . "/logs.log";
        file_put_contents($file_name, implode("\n", $this->bugsInsert));

        $config = [];
        if (file_exists($this->baseDir . '/session.json')) {
            $config = json_decode(file_get_contents($this->baseDir . '/session.json'), true);
        }

        $config[$this->filename] = $this->config;

        file_put_contents($this->baseDir . '/session.json', json_encode($config));
    }

    function fopen(){
        $this->handle = @fopen($this->filename, "r");
        return $this;
    }

    public function readln(){
        fseek($this->handle, $this->seek);
        $buffer = fgets($this->handle, 4096);
        // $buffer = iconv("UTF-8", "WINDOWS-1251", $buffer);
        $buffer = mb_convert_encoding($buffer, "WINDOWS-1251", "UTF-8");

        $this->seek = ftell($this->handle);
        return $buffer;
    }

    function fread(){
        if (feof($this->handle)) return $this;

        $buffer = $this->addSplit(
                    $this->clearBuffer(
                        $this->readln()
                ));
        
        $this->bfr .= $buffer;

        return $this;
    }

    function clearBuffer($str){
        $str = str_replace('*/;','*/', $str);
        $str = preg_replace('/^--.{0,}$/', '', $str);
        $str = preg_replace('/\/\*.{0,}?\*\//', '', $str);
        $str = trim($str);

        return trim(trim($str));
    }

    function setSeek(){
        fseek($this->handle, $this->seek);
    }

    function addSplit($val){
        $val = str_replace('CREATE TABLE', '*|SPLIT|*CREATE TABLE', $val);
        $val = str_replace('INSERT INTO', '*|SPLIT|*INSERT INTO', $val);
        // $val = str_replace('VALUES', '*|SPLIT|*VALUES', $val);

        if (substr($val, -1) == ';') {
            $val .= '*|SPLIT|*';
        }

        return $val;
    }

    function bufferToArray(){
        $array = explode('*|SPLIT|*', $this->bfr);

        foreach($array as $key => $value){
            if (trim($value)=='')
                unset($array[$key]);
        } 
        
        return array_values($array);
    }



    public function eof(){
        return feof($this->handle);
    } 
    

    function parsing(){
        $buffer = $this->fread()->bfr;
        if (count($this->bufferToArray())>1){
            $bufferArray = $this->bufferToArray();

            for($i = 0; $i<count($bufferArray)-1; $i++){
                $value = $bufferArray[$i];

                $command = strtoupper(stristr(str_replace('(', ' ', $value), ' ', true));
                
                if (!isset($this->queris[$command])) $this->queris[$command] = [];
                array_push($this->queris[$command], $value);

                if (isset($this->queris['INSERT']))
                if (count($this->queris['INSERT'])>$this->maxInsert){
                    $this->insertTo();
                }
            }

            $this->bfr = $bufferArray[count($bufferArray)-1];
        }

        if (isset($this->queris['INSERT']) && count($this->queris['INSERT'])!=0 && $this->eof()==true){
            echo "Файл прочитан полностью а инсерта не набралось";
            tlg('Не удалось прочитать файл полностью');
            $this->insertTo();
        }
    }

    function getStringBetween($str, $from, $to){
		$sub = substr($str, strpos($str,$from)+strlen($from),strlen($str));
		return substr($sub,0,strpos($sub,$to));
	}

    
    function insertTo(){
        $tableNames = [];


        $this->counts += count($this->queris['INSERT']);
        
        $insert = "";
        $tables = [];
        foreach ($this->queris['INSERT'] as $query) {

            if ($insert==''){
                $insert = substr($query,0,-1);
            } else {
                $array = explode("VALUES", $query);
                $tables[$this->getStringBetween($array[0], '`', '`')] = 1;

                $insert .=  ','.substr($array[1], 0, -1) ;
            }
        }




        foreach ($tables as $tableName => $__) {
            if (!isset($this->config['removeTable']) || !in_array($tableName, $this->config['removeTable'])){
                if ($this->mysqli->query("DROP TABLE `$tableName`")){
                    tlg("DROP TABLE `$tableName` -- OK");
                    echo "Табличка $tableName - удалена \n";

                    if (!isset($this->config['removeTable'])) $this->config['removeTable'] = [];
                    array_push($this->config['removeTable'], $tableName);
                }
            }

            if (!isset($this->config['createTable']) || !in_array($tableName, $this->config['createTable'])){
                if (isset($this->queris['CREATE']) && count($this->queris['CREATE']) != 0) {
                    foreach ($this->queris['CREATE'] as $query) {
                        if ($this->mysqli->query($query)) echo "CREATE OK\n";
                    }

                    echo "Табличка $tableName - создана \n";
                    tlg("CREATE TABLE `$tableName` -- OK");

                    if (!isset($this->config['createTable'])) $this->config['createTable'] = [];

                    array_push($this->config['createTable'], $tableName);
                }
            }
        }



        // $tableName = basename($this->filename, '.sql');
        

        if (!$this->setSET && isset($this->queris['SET']) && count($this->queris['SET'])!=0){
            foreach($this->queris['SET'] as $query){
                if ($this->mysqli->query($query)) echo "SET OK\n";
            }
            $this->setSET = true;
        }

        





        if ($this->mysqli->query($insert)){
            echo $tableName . ' | ' . $this->counts . " | OK " . stristr($insert, '(', true) . "\n";
        } else array_push($this->bugsInsert, $insert);



        if (count($this->bugsInsert)>$this->dieAfterCountBugs){
            tlg('Надо наверно табличку очистить');
            tlg(print_r($this->config, true));
            tlg(print_r($tables, true));
            

            die('Надо наверно табличку очистить');
        }
        $this->queris['INSERT'] = [];






        if ($this->needSaveConfig==0){
            $this->saveSession();
            $this->needSaveConfig = 10;
        }

        $this->needSaveConfig--;
    }

    function fclose(){
        fclose($this->handle);
        return $this;
    }
}


$folderPath = conf('folderPath');

$dir = array_filter(scandir($folderPath), function($el){
    return $el!='.' && $el!='..';
});

foreach($dir as $file){
    if (explode('.', $file)[count(explode('.', $file)) - 1] != 'sql') continue;
    tlg("Use file - $file");
    echo "Use file - $folderPath$file\n";
    $pimp = new phpImport($folderPath . $file, true);

    $pimp->fopen();

    while(!$pimp->eof())
        $pimp->parsing();

    $pimp->fclose();
}
<?php
namespace Doctrine\DBAL\Driver\Drizzle;

use Doctrine\DBAL\Driver\Statement as StatementInterface;
use PDO;

class Statement implements \IteratorAggregate, StatementInterface
{
    private $conn;
    private $drizzle;
    private $statement;
    private $result;
    private $boundParams = array();
    private $isBuffered = false;
    
    protected $_defaultFetchStyle = PDO::FETCH_BOTH;

    public function __construct(\Drizzle $drizzle, \DrizzleCon $conn, $statement)
    {
        $this->conn      = $conn;
        $this->drizzle   = $drizzle;
        $this->statement = $statement;
    }

    public function unimplemented()
    {
        $bt = debug_backtrace();
        throw new Exception($bt[2]['class'].'::'.$bt[2]['function'].'=> '.serialize(func_get_args()).', '.$this->statement);
    }

    public function getIterator()
    {
        $data = $this->fetchAll($this->_defaultFetchStyle);
        return new \ArrayIterator($data);
    }

    public function bindValue($param, $value, $type = null)
    {
        return $this->bindParam($param, $value, $type);
    }

    public function bindParam($column, &$variable, $type = null)
    {
        $this->boundParams[$column] = array('type' => $type,
                                            'value' => &$variable,
                                        );
    }

    public function errorCode()
    {
        return $this->conn->errorCode();
    }

    public function errorInfo()
    {
        return $this->conn->errorString();
    }

    public function execute($params = null)
    {
        if (!is_array($params)) {
            $params = $this->boundParams;
        } else {
            $tmp = array();
            foreach ($params as $param => $value) {
                $tmp[] = array('type' => null,
                               'value' => $value);
            }
            $params = $tmp;
        }
        if (count($params) > 0) {
            $query = "";
            $statement = $this->statement;
            while (($pos = strpos($statement, "?")) !== false) {
                $part = substr($statement,0,$pos);

                $param = array_shift($params);
                $query.=$part;

                
                if ($param['value'] === null) {
                    $query.='NULL';
                } elseif (!isset($param['type']) || in_array($param['type'], array(\PDO::PARAM_STR, \PDO::PARAM_LOB))) {
                    $query.="'".$this->drizzle->escapeString($param['value'])."'";
                } elseif ($param['type'] == \PDO::PARAM_NULL) {
                    $query.='NULL';
                } elseif ($param['type'] == \PDO::PARAM_BOOL || $param['type'] == \PDO::PARAM_INT) {
                    $query.=(int)$param['value'];
                } else {
                    throw new Exception('Param type '.$param['type'].' is unsupported');
                }

                $statement = substr($statement, $pos+1);
            }   
            $query.=$statement;
        } else {
            $query = $this->statement;
        }
        $this->result = @$this->conn->query($query);
        if (!$this->result) {
            throw new Exception($query.': '.$this->conn->error(), $this->conn->errorCode());
        }
        $this->result->buffer();
        return true;
    }

    public function rowCount()
    {
        return $this->result->affectedRows();
    }

    public function closeCursor()
    {
        return;
        $this->map = NULL;
        $this->boundParams = array();
        unset($this->result);
    }

    public function columnCount()
    {
        return $this->result->columnCount();
    }
    
    /**
     * {@inheritdoc}
     */
    public function setFetchMode($fetchMode = PDO::FETCH_BOTH)
    {
        $this->_defaultFetchStyle = $fetchMode;
    }

    public function fetch($fetchStyle = null)
    {
        $fetchStyle = $fetchStyle ?: $this->_defaultFetchStyle;
        if (in_array($fetchStyle, array(\PDO::FETCH_BOTH, \PDO::FETCH_ASSOC)) && !isset($this->map)) {
            $this->map = array();
            while (($column=$this->result->columnNext()) != NULL) {
                $this->map[] = $column->name();
            }
        }
        $row = $this->result->rowNext();
        if (!isset($row)) {
            return $row;
        }
        switch ($fetchStyle) {
            case \PDO::FETCH_BOTH:
                $both = array();
                foreach ($row as $index => $value) {
                    $both[$index] = $value;
                    $both[$this->map[$index]] = $value;
                }
                return $both;
            case \PDO::FETCH_ASSOC:
                $named = array();
                foreach ($row as $index => $value) {
                    $named[$this->map[$index]] = $value;
                }
                return $named;
            case \PDO::FETCH_NUM:
                return $row;
            default:
                throw new Exception("Given Fetch-Style " . $fetchStyle . " is not supported.");
        }
    }

    public function fetchAll($fetchStyle = null)
    {
        $result = array();
        
        while ($row = $this->fetch($fetchStyle)) {
            $result[] = $row;
        }
        return $result;
    }

    public function fetchColumn($columnIndex = 0)
    {
        $row = $this->fetch(\PDO::FETCH_NUM);
        return $row[$columnIndex];
    }

    public function getResult()
    {
        return $this->result;
    }
}

<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the LGPL. For more information, see
 * <http://www.doctrine-project.org>.
 */
namespace Doctrine\DBAL\Driver\Drizzle;

/**
 * @author Andreas Streichardt <andreas.streichardt@gmail.com>
 */
class Connection implements \Doctrine\DBAL\Driver\Connection
{
    /**
     * @var \Drizzle
     * @access protected
     */
    protected $_drizzle;
    
    /**
     * @var \DrizzleCon
     * @access protected
     */
    protected $_dbh;

    /**
     * @var \Doctrine\DBAL\Driver\Drizzle\Statement
     * @access private
     */
    private $lastStatement;

    public function __construct($host, $port, $db, $username, $password)
    {
        $this->_drizzle = drizzle_create();
        $this->_dbh = drizzle_con_add_tcp($this->_drizzle, $host, (int)$port, $username, $password, $db, 0);
    }

    /**
     * {@inheritdoc}
     */
    public function prepare($prepareString)
    {
        $this->lastStatement = new Statement($this->_drizzle, $this->_dbh, $prepareString);
        return $this->lastStatement;
    }
    
    /**
     * {@inheritdoc}
     */
    public function query()
    {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();
        return $stmt;
    }
    
    /**
     * {@inheritdoc}
     */
    public function quote($input, $type=\PDO::PARAM_STR)
    {
        if (is_int($input) || is_float($input)) {
            return $input;
        }
        return "'". $this->_drizzle->escapeString($input) ."'";
    }
    
    /**
     * {@inheritdoc}
     */
    public function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();
        return $stmt->rowCount();
    }

    /**
     * {@inheritdoc}
     */
    public function lastInsertId($name = null)
    {
        return $this->lastStatement->getResult()->insertId();
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction()
    {
        $this->_dbh->query('START TRANSACTION');
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function rollBack()
    {
        $this->_dbh->query('ROLLBACK');
        return true;
    }
    
    /**
     * {@inheritdoc}
     */
    public function commit()
    {
        $this->_dbh->query('COMMIT');
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function errorCode()
    {
        return $this->_dbh->errorCode();
    }

    /**
     * {@inheritdoc}
     */
    public function errorInfo()
    {
        return $this->_dbh->errorString();
    }
}

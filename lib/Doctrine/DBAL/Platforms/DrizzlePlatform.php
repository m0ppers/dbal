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

namespace Doctrine\DBAL\Platforms;

use Doctrine\DBAL\DBALException,
    Doctrine\DBAL\Schema\TableDiff,
    Doctrine\DBAL\Schema\Index,
    Doctrine\DBAL\Schema\Table;

use Doctrine\DBAL\Driver\Drizzle\Exception;

/**
 * The MySqlPlatform provides the behavior, features and SQL dialect of the
 * MySQL database platform. This platform represents a MySQL 5.0 or greater platform that
 * uses the InnoDB storage engine.
 *
 * @since 2.0
 * @author Roman Borschel <roman@code-factory.org>
 * @author Benjamin Eberlei <kontakt@beberlei.de>
 * @todo Rename: MySQLPlatform
 */
class DrizzlePlatform extends AbstractPlatform
{
    /**
     * Gets the character used for identifier quoting.
     *
     * @return string
     * @override
     */
    public function getIdentifierQuoteCharacter()
    {
        return '`';
    }

    /**
     * Returns the regular expression operator.
     *
     * @return string
     * @override
     */
    public function getRegexpExpression()
    {
        return 'RLIKE';
    }

    /**
     * Returns global unique identifier
     *
     * @return string to get global unique identifier
     * @override
     */
    public function getGuidExpression()
    {
        return 'UUID()';
    }

    /**
     * returns the position of the first occurrence of substring $substr in string $str
     *
     * @param string $substr    literal string to find
     * @param string $str       literal string
     * @param int    $pos       position to start at, beginning of string by default
     * @return integer
     */
    public function getLocateExpression($str, $substr, $startPos = false)
    {
        if ($startPos == false) {
            return 'LOCATE(' . $substr . ', ' . $str . ')';
        } else {
            return 'LOCATE(' . $substr . ', ' . $str . ', '.$startPos.')';
        }
    }

    /**
     * Returns a series of strings concatinated
     *
     * concat() accepts an arbitrary number of parameters. Each parameter
     * must contain an expression or an array with expressions.
     *
     * @param string|array(string) strings that will be concatinated.
     * @override
     */
    public function getConcatExpression()
    {
        $args = func_get_args();
        return 'CONCAT(' . join(', ', (array) $args) . ')';
    }

    public function getDateDiffExpression($date1, $date2)
    {
        return 'DATEDIFF(' . $date1 . ', ' . $date2 . ')';
    }

    public function getDateAddDaysExpression($date, $days)
    {
        return 'DATE_ADD(' . $date . ', INTERVAL ' . $days . ' DAY)';
    }

    public function getDateSubDaysExpression($date, $days)
    {
        return 'DATE_SUB(' . $date . ', INTERVAL ' . $days . ' DAY)';
    }

    public function getDateAddMonthExpression($date, $months)
    {
        return 'DATE_ADD(' . $date . ', INTERVAL ' . $months . ' MONTH)';
    }

    public function getDateSubMonthExpression($date, $months)
    {
        return 'DATE_SUB(' . $date . ', INTERVAL ' . $months . ' MONTH)';
    }

    public function getListDatabasesSQL()
    {
        return 'SHOW DATABASES';
    }

    public function getListTableConstraintsSQL($table)
    {
        return 'SHOW INDEX FROM ' . $table;
    }

    /**
     * Two approaches to listing the table indexes. The information_schema is
     * prefered, because it doesn't cause problems with SQL keywords such as "order" or "table".
     *
     * @param string $table
     * @param string $currentDatabase
     * @return string
     */
    public function getListTableIndexesSQL($table, $currentDatabase = null)
    {
        if (!$currentDatabase) {
            throw new Exception('Must have database');
        }
        
        return "SELECT DATA_DICTIONARY.INDEXES.TABLE_NAME as `Table`, IF(DATA_DICTIONARY.INDEXES.IS_UNIQUE='YES',0,1) AS Non_Unique,DATA_DICTIONARY.INDEXES.INDEX_NAME AS Key_name,SEQUENCE_IN_INDEX AS Seq_in_index, COLUMN_NAME AS Column_Name,'A' AS Collation,DATA_DICTIONARY.INDEXES.IS_NULLABLE as `Null`,INDEX_TYPE as Index_Type FROM DATA_DICTIONARY.INDEXES,DATA_DICTIONARY.INDEX_PARTS WHERE DATA_DICTIONARY.INDEXES.INDEX_NAME=DATA_DICTIONARY.INDEX_PARTS.INDEX_NAME AND DATA_DICTIONARY.INDEXES.TABLE_NAME=DATA_DICTIONARY.INDEX_PARTS.TABLE_NAME AND DATA_DICTIONARY.INDEXES.TABLE_SCHEMA=DATA_DICTIONARY.INDEX_PARTS.TABLE_SCHEMA AND DATA_DICTIONARY.INDEXES.TABLE_NAME = '".$table."' AND DATA_DICTIONARY.INDEXES.TABLE_SCHEMA = '".$currentDatabase."'";
    }

    public function getListViewsSQL($database)
    {
        return "SELECT * FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '".$database."'";
    }

    public function getListTableForeignKeysSQL($table, $database = null)
    {
        // XXX multiple column names in *_COLUMNS
        $sql = "SELECT DISTINCT CONSTRAINT_NAME,CONSTRAINT_COLUMNS,REFERENCED_TABLE_NAME,REFERENCED_TABLE_COLUMNS,UPDATE_RULE,DELETE_RULE FROM DATA_DICTIONARY.FOREIGN_KEYS WHERE CONSTRAINT_TABLE = '".$table."'";
        if ($database) {
            $sql .= " AND CONSTRAINT_SCHEMA = '".$database."'";
        }
        return $sql;
    }

    public function getCreateViewSQL($name, $sql)
    {
        return 'CREATE VIEW ' . $name . ' AS ' . $sql;
    }

    public function getDropViewSQL($name)
    {
        return 'DROP VIEW '. $name;
    }

    /**
     * Gets the SQL snippet used to declare a VARCHAR column on the MySql platform.
     *
     * @params array $field
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed)
    {
        return $fixed ? ($length ? 'CHAR(' . $length . ')' : 'CHAR(255)')
                : ($length ? 'VARCHAR(' . $length . ')' : 'VARCHAR(255)');
    }

    /** @override */
    public function getClobTypeDeclarationSQL(array $field)
    {
        if ( ! empty($field['length']) && is_numeric($field['length'])) {
            $length = $field['length'];
            if ($length <= 255) {
                return 'TINYTEXT';
            } else if ($length <= 65532) {
                return 'TEXT';
            } else if ($length <= 16777215) {
                return 'MEDIUMTEXT';
            }
        }
        return 'LONGTEXT';
    }

    /**
     * @override
     */
    public function getDateTimeTypeDeclarationSQL(array $fieldDeclaration)
    {
        if (isset($fieldDeclaration['version']) && $fieldDeclaration['version'] == true) {
            return 'TIMESTAMP';
        } else {
            return 'DATETIME';
        }
    }

    /**
     * @override
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'DATE';
    }

    /**
     * @override
     */
    public function getTimeTypeDeclarationSQL(array $fieldDeclaration)
    {
        return 'TIME';
    }

    /**
     * @override
     */
    public function getBooleanTypeDeclarationSQL(array $field)
    {
        return 'BOOLEAN';
    }

    /**
     * Obtain DBMS specific SQL code portion needed to set the COLLATION
     * of a field declaration to be used in statements like CREATE TABLE.
     *
     * @param string $collation   name of the collation
     * @return string  DBMS specific SQL code portion needed to set the COLLATION
     *                 of a field declaration.
     */
    public function getCollationFieldDeclaration($collation)
    {
        return 'COLLATE ' . $collation;
    }

    /**
     * Whether the platform prefers identity columns for ID generation.
     * MySql prefers "autoincrement" identity columns since sequences can only
     * be emulated with a table.
     *
     * @return boolean
     * @override
     */
    public function prefersIdentityColumns()
    {
        return true;
    }

    /**
     * Whether the platform supports identity columns.
     * MySql supports this through AUTO_INCREMENT columns.
     *
     * @return boolean
     * @override
     */
    public function supportsIdentityColumns()
    {
        return true;
    }

    public function supportsInlineColumnComments()
    {
        return true;
    }

    public function getShowDatabasesSQL()
    {
        return 'SHOW DATABASES';
    }

    public function getListTablesSQL()
    {
        return "SHOW TABLES";
    }

    public function getListTableColumnsSQL($table, $database = null)
    {
        if ($database) {
            return "SELECT COLUMN_NAME AS Field, DATA_TYPE AS Type, IS_NULLABLE AS `Null`, ".
                "COLUMN_DEFAULT AS `Default`,IS_AUTO_INCREMENT as 'auto_increment',IF(DATA_TYPE='VARCHAR' OR DATA_TYPE='CHAR',CHARACTER_MAXIMUM_LENGTH,NULL) as length," .
                "NUMERIC_SCALE as scale, NUMERIC_PRECISION as `precision`, COLUMN_COMMENT as comment " .
                   "FROM DATA_DICTIONARY.COLUMNS WHERE TABLE_SCHEMA = '" . $database . "' AND TABLE_NAME = '" . $table . "'";
        } else {
            throw new Exception('Must have database');
        }
    }

    /**
     * create a new database
     *
     * @param string $name name of the database that should be created
     * @return string
     * @override
     */
    public function getCreateDatabaseSQL($name)
    {
        return 'CREATE DATABASE ' . $name;
    }

    /**
     * drop an existing database
     *
     * @param string $name name of the database that should be dropped
     * @return string
     * @override
     */
    public function getDropDatabaseSQL($name)
    {
        return 'DROP DATABASE ' . $name;
    }

    /**
     * create a new table
     *
     * @param string $tableName   Name of the database that should be created
     * @param array $columns  Associative array that contains the definition of each field of the new table
     *                       The indexes of the array entries are the names of the fields of the table an
     *                       the array entry values are associative arrays like those that are meant to be
     *                       passed with the field definitions to get[Type]Declaration() functions.
     *                          array(
     *                              'id' => array(
     *                                  'type' => 'integer',
     *                                  'unsigned' => 1
     *                                  'notnull' => 1
     *                                  'default' => 0
     *                              ),
     *                              'name' => array(
     *                                  'type' => 'text',
     *                                  'length' => 12
     *                              ),
     *                              'password' => array(
     *                                  'type' => 'text',
     *                                  'length' => 12
     *                              )
     *                          );
     * @param array $options  An associative array of table options:
     *                          array(
     *                              'comment' => 'Foo',
     *                              'charset' => 'utf8',
     *                              'collate' => 'utf8_unicode_ci',
     *                              'engine' => 'innodb',
     *                              'foreignKeys' => array(
     *                                  new ForeignKeyConstraint(),
     *                                  new ForeignKeyConstraint(),
     *                                  new ForeignKeyConstraint(),
     *                                  // etc
     *                              )
     *                          );
     *
     * @return void
     * @override
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = array())
    {
        $queryFields = $this->getColumnDeclarationListSQL($columns);

        if (isset($options['uniqueConstraints']) && ! empty($options['uniqueConstraints'])) {
            foreach ($options['uniqueConstraints'] as $index => $definition) {
                $queryFields .= ', ' . $this->getUniqueConstraintDeclarationSQL($index, $definition);
            }
        }

        // add all indexes
        if (isset($options['indexes']) && ! empty($options['indexes'])) {
            foreach($options['indexes'] as $index => $definition) {
                $queryFields .= ', ' . $this->getIndexDeclarationSQL($index, $definition);
            }
        }

        // attach all primary keys
        if (isset($options['primary']) && ! empty($options['primary'])) {
            $keyColumns = array_unique(array_values($options['primary']));
            $queryFields .= ', PRIMARY KEY(' . implode(', ', $keyColumns) . ')';
        }

        $query = 'CREATE ';
        if (!empty($options['temporary'])) {
            $query .= 'TEMPORARY ';
        }
        $query.= 'TABLE ' . $tableName . ' (' . $queryFields . ')';

        $optionStrings = array();

        if (isset($options['comment'])) {
            $optionStrings['comment'] = 'COMMENT = ' . $options['comment'];
        }
        if (isset($options['charset'])) {
            $optionStrings['charset'] = 'DEFAULT CHARACTER SET ' . $options['charset'];
            if (isset($options['collate'])) {
                $optionStrings['charset'] .= ' COLLATE ' . $options['collate'];
            }
        }

        // get the type of the table
        if (isset($options['engine'])) {
            $optionStrings[] = 'ENGINE = ' . $options['engine'];
        } else {
            // default to innodb
            $optionStrings[] = 'ENGINE = InnoDB';
        }

        if ( ! empty($optionStrings)) {
            $query.= ' '.implode(' ', $optionStrings);
        }
        $sql[] = $query;

        if (isset($options['foreignKeys'])) {
            foreach ((array) $options['foreignKeys'] as $definition) {
                $sql[] = $this->getCreateForeignKeySQL($definition, $tableName);
            }
        }

        return $sql;
    }

    /**
     * Gets the SQL to alter an existing table.
     *
     * @param TableDiff $diff
     * @return array
     */
    public function getAlterTableSQL(TableDiff $diff)
    {
        $queryParts = array();
        if ($diff->newName !== false) {
            $queryParts[] =  'RENAME TO ' . $diff->newName;
        }

        foreach ($diff->addedColumns AS $fieldName => $column) {
            $columnArray = $column->toArray();
            $columnArray['comment'] = $this->getColumnComment($column);
            $queryParts[] = 'ADD ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->removedColumns AS $column) {
            $queryParts[] =  'DROP ' . $column->getQuotedName($this);
        }

        foreach ($diff->changedColumns AS $columnDiff) {
            /* @var $columnDiff Doctrine\DBAL\Schema\ColumnDiff */
            $column = $columnDiff->column;
            $columnArray = $column->toArray();
            $columnArray['comment'] = $this->getColumnComment($column);
            $queryParts[] =  'CHANGE ' . ($columnDiff->oldColumnName) . ' '
                    . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->renamedColumns AS $oldColumnName => $column) {
            $columnArray = $column->toArray();
            $columnArray['comment'] = $this->getColumnComment($column);
            $queryParts[] =  'CHANGE ' . $oldColumnName . ' '
                    . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        $sql = array();
        if (count($queryParts) > 0) {
            $sql[] = 'ALTER TABLE ' . $diff->name . ' ' . implode(", ", $queryParts);
        }
        $sql = array_merge(
            $this->getPreAlterTableIndexForeignKeySQL($diff),
            $sql,
            $this->getPostAlterTableIndexForeignKeySQL($diff)
        );
        return $sql;
    }

    /**
     * Obtain DBMS specific SQL code portion needed to declare an integer type
     * field to be used in statements like CREATE TABLE.
     *
     * @param string  $name   name the field to be declared.
     * @param string  $field  associative array with the name of the properties
     *                        of the field being declared as array indexes.
     *                        Currently, the types of supported field
     *                        properties are as follows:
     *
     *                       unsigned
     *                        Boolean flag that indicates whether the field
     *                        should be declared as unsigned integer if
     *                        possible.
     *
     *                       default
     *                        Integer value to be used as default for this
     *                        field.
     *
     *                       notnull
     *                        Boolean flag that indicates whether this field is
     *                        constrained to not be set to null.
     * @return string  DBMS specific SQL code portion that should be used to
     *                 declare the specified field.
     * @override
     */
    public function getIntegerTypeDeclarationSQL(array $field)
    {
        return 'INT' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /** @override */
    public function getBigIntTypeDeclarationSQL(array $field)
    {
        return 'BIGINT' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /** @override */
    public function getSmallIntTypeDeclarationSQL(array $field)
    {
        return 'SMALLINT' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /** @override */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef)
    {
        $autoinc = '';
        if ( ! empty($columnDef['autoincrement'])) {
            $autoinc = ' AUTO_INCREMENT';
        }
        $unsigned = (isset($columnDef['unsigned']) && $columnDef['unsigned']) ? ' UNSIGNED' : '';

        return $unsigned . $autoinc;
    }

    /**
     * Return the FOREIGN KEY query section dealing with non-standard options
     * as MATCH, INITIALLY DEFERRED, ON UPDATE, ...
     *
     * @param ForeignKeyConstraint $foreignKey
     * @return string
     * @override
     */
    public function getAdvancedForeignKeyOptionsSQL(\Doctrine\DBAL\Schema\ForeignKeyConstraint $foreignKey)
    {
        $query = '';
        if ($foreignKey->hasOption('match')) {
            $query .= ' MATCH ' . $foreignKey->getOption('match');
        }
        $query .= parent::getAdvancedForeignKeyOptionsSQL($foreignKey);
        return $query;
    }

    /**
     * Gets the SQL to drop an index of a table.
     *
     * @param Index $index           name of the index to be dropped
     * @param string|Table $table          name of table that should be used in method
     * @override
     */
    public function getDropIndexSQL($index, $table=null)
    {
        if($index instanceof Index) {
            $indexName = $index->getQuotedName($this);
        } else if(is_string($index)) {
            $indexName = $index;
        } else {
            throw new \InvalidArgumentException('MysqlPlatform::getDropIndexSQL() expects $index parameter to be string or \Doctrine\DBAL\Schema\Index.');
        }

        if($table instanceof Table) {
            $table = $table->getQuotedName($this);
        } else if(!is_string($table)) {
            throw new \InvalidArgumentException('MysqlPlatform::getDropIndexSQL() expects $table parameter to be string or \Doctrine\DBAL\Schema\Table.');
        }

        if ($index instanceof Index && $index->isPrimary()) {
            // mysql primary keys are always named "PRIMARY",
            // so we cannot use them in statements because of them being keyword.
            return $this->getDropPrimaryKeySQL($table);
        }

        return 'DROP INDEX ' . $indexName . ' ON ' . $table;
    }

    /**
     * @param Index $index
     * @param Table $table
     */
    protected function getDropPrimaryKeySQL($table)
    {
        return 'ALTER TABLE ' . $table . ' DROP PRIMARY KEY';
    }

    public function getSetTransactionIsolationSQL($level)
    {
        return 'SET SESSION TRANSACTION ISOLATION LEVEL ' . $this->_getTransactionIsolationLevelSQL($level);
    }

    /**
     * Get the platform name for this instance.
     *
     * @return string
     */
    public function getName()
    {
        return 'drizzle';
    }

    public function getReadLockSQL()
    {
        return 'LOCK IN SHARE MODE';
    }

    protected function initializeDoctrineTypeMappings()
    {
        $this->doctrineTypeMapping = array(
            'integer'       => 'integer',
            'bigint'        => 'bigint',
            'date'          => 'date',
            'datetime'      => 'datetime',
            'timestamp'     => 'datetime',
            'time'          => 'time',
            'varchar'       => 'string',
            'text'          => 'text',
            'boolean'       => 'boolean',
            'double'        => 'float',
            'decimal'       => 'decimal',
        );
    }

    public function getVarcharMaxLength()
    {
        return 65535;
    }

    protected function getReservedKeywordsClass()
    {
        return 'Doctrine\DBAL\Platforms\Keywords\MySQLKeywords';
    }

    /**
     * Get SQL to safely drop a temporary table WITHOUT implicitly committing an open transaction.
     *
     * MySQL commits a transaction implicitly when DROP TABLE is executed, however not
     * if DROP TEMPORARY TABLE is executed.
     *
     * @throws \InvalidArgumentException
     * @param $table
     * @return string
     */
    public function getDropTemporaryTableSQL($table)
    {
        if ($table instanceof \Doctrine\DBAL\Schema\Table) {
            $table = $table->getQuotedName($this);
        } else if(!is_string($table)) {
            throw new \InvalidArgumentException('getDropTableSQL() expects $table parameter to be string or \Doctrine\DBAL\Schema\Table.');
        }

        return 'DROP TEMPORARY TABLE ' . $table;
    }

    /**
     * Gets the SQL Snippet used to declare a BLOB column type.
     */
    public function getBlobTypeDeclarationSQL(array $field)
    {
        return 'LONGBLOB';
    }
    
    public function supportsViews()
    {
        return false;
    }

    public function convertBooleans($item)
    {
        if (is_array($item)) {
            foreach ($item as $key => $value) {
                $item[$key] = $value ? 'TRUE' : 'FALSE';
            }
        } elseif (isset($item)) {
            $item = $item ? 'TRUE' : 'FALSE';
        }
        return $item;
    }
}

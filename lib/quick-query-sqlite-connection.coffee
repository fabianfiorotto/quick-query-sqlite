sql = require './sql-proxy'

{Emitter} = require 'atom'

class QuickQuerySqliteColumn
  type: 'column'
  child_type: null
  constructor: (@table,row) ->
    @connection = @table.connection
    @name = row['name']
    @primary_key = row['pk'] == 1
    @datatype = row['type']
    @default = row['dflt_value']
    @nullable = row['notnull'] != 1
  toString: ->
    @name
  parent: ->
    @table
  children: (callback)->
    callback([])

class QuickQuerySqliteTable
  type: 'table'
  child_type: 'column'
  constructor: (@database,row,fields) ->
    @connection = @database.connection
    @name = row[fields[0].name]
  toString: ->
    @name
  parent: ->
    @database
  children: (callback)->
    @connection.getColumns(@,callback)
class QuickQuerySqliteDatabase
  type: 'database'
  child_type: 'table'
  constructor: (@connection,name) ->
    @name = name
  toString: ->
    @name
  parent: ->
    @connection
  children: (callback)->
    @connection.getTables(@,callback)

module.exports =
class QuickQuerySqliteConnection

  fatal: false
  connection: null
  protocol: 'sqlite'
  type: 'connection'
  child_type: 'database'

  allowEdition: false
  @fromFilesystem: true
  @fileExtencions: ['db','sqlite','sqlite3','sdb','sdb3']

  n_types: 'NUMERIC INTEGER REAL'.split /\s+/
  s_types: 'TEXT BLOB CHAR'.split /\s+/

  constructor: (@info)->
    @emitter = new Emitter()
    @autosave = @info.autosave? && @info.autosave

  connect: (callback)->
    @connection = sql.createConnection(@info, callback)

  serialize: ->
    file: @info.file
    protocol: @protocol

  dispose: ->
    @close()

  close: ->
    @connection.close();

  query: (text,callback) ->
    message = null
    try
      @connection.query text, ({type, content, fields, rows}) =>
        if fields? && rows?
          callback(null,rows,fields)
        else if @autosave
          @connection.save =>
            callback {type, content}
        else
          callback {type, content}
    catch e
      message = { type: 'error' , content: e.message }
      callback(message)

  objRowsMap: (rows,fields,callback)->
    rows.map (r,i) =>
      row = {}
      row[field.name] = r[j] for field,j in fields
      if callback? then callback(row) else row

  setDefaultDatabase: (database)->

  getDefaultDatabase: -> 'main'

  parent: -> @

  children: (callback)->
    @getDatabases (databases) -> callback(databases)

  getDatabases: (callback) ->
    text = "PRAGMA database_list"
    @query text, (err,rows, fields) =>
      if !err
        databases = @objRowsMap rows, fields, (row) =>
          new QuickQuerySqliteDatabase(@,row['name'])
        callback(databases)

  getTables: (database,callback) ->
    if database.name == 'temp'
      text = "SELECT name FROM sqlite_temp_master WHERE type='table'"
    else
      text = "SELECT name FROM sqlite_master WHERE type='table'"
    @query text, (err,rows, fields) =>
      if !err
        tables = @objRowsMap rows,fields, (row) =>
          new QuickQuerySqliteTable(database,row,fields)
        callback(tables)

  getColumns: (table,callback) ->
    text = "PRAGMA table_info('#{table.name}')"
    @query text, (err,rows, fields) =>
      if !err
        columns = @objRowsMap rows, fields, (row) =>
          new QuickQuerySqliteColumn(table,row)
        table.columns = columns
        callback(columns)

  hiddenDatabase: (database) ->

  simpleSelect: (table, columns = '*') ->
    if columns != '*'
      columns = columns.map (col) =>
        @escapeId(col.name)
      columns = "\n "+columns.join(",\n ") + "\n"
    table_name = @escapeId(table.name)
    database_name = @escapeId(table.database.name)
    "SELECT #{columns} FROM #{database_name}.#{table_name} LIMIT 1000"

  save: ->
    @connection.save();

  createDatabase: (model,info)->
    "Not supported"

  createTable: (model,info)->
    database = @escapeId(model.name)
    table = @escapeId(info.name)
    "CREATE TABLE #{database}.#{table} ( \n"+
    " \"id\" INTEGER PRIMARY KEY NOT NULL\n"+
    ");"

  createColumn: (model,info)->
    database = @escapeId(model.database.name)
    table = @escapeId(model.name)
    column = @escapeId(info.name)
    nullable = if info.nullable then 'NULL' else 'NOT NULL'
    dafaultValue = @escape(info.default,info.datatype) || 'NULL'
    "ALTER TABLE #{database}.#{table} ADD COLUMN #{column}"+
    " #{info.datatype} #{nullable} DEFAULT #{dafaultValue};"

  alterTable: (model,delta)->
    database = @escapeId(model.database.name)
    newName = @escapeId(delta.new_name)
    oldName = @escapeId(delta.old_name)
    "ALTER TABLE #{database}.#{oldName} RENAME TO #{newName};"

  renameColumn: (model,delta)->
    database = @escapeId(model.table.database.name)
    table = @escapeId(model.table.name)
    column = @escapeId(model.name)
    new_name = @escapeId(delta.new_name);
    "ALTER TABLE #{database}.#{table} RENAME COLUMN #{column} TO #{new_name};"

  onlyNameChanged: (model, delta) ->
    delta.new_name != model.name &&
    delta.default == model.default &&
    delta.nullable == model.nullable

  alterColumn: (model,delta)->
    return @renameColumn(model, delta) if @onlyNameChanged(model, delta)
    table_name = model.table.name
    tp_table = @escapeId(table_name+"_backup")
    table = @escapeId(table_name)
    def_col = (col)=>
       if col.name == model.name
        dafaultValue =  @escape(delta.default,delta.datatype)
        delta.new_name+" "+delta.datatype+
        (if col.primary_key  then " PRIMARY KEY" else "") +
        (if !delta.nullable then " NOT NULL" else " NULL")+
        (if delta.default then " DEFAULT #{dafaultValue}" else "") #TODO default must be escaped
       else
        dafaultValue = @escape(col.default,col.type)
        col.name + " " + col.type +
        (if col.primary_key then " PRIMARY KEY" else "") +
        (if col.nullable then " NOT NULL" else " NULL")+
        (if col.default then " DEFAULT #{@escape(col.default)}" else "")

    columns = model.table.columns.map((col)->
       if col.name == model.name then delta.new_name else col.name
    ).join(',')
    columns_def =  model.table.columns.map(def_col).join(',')

    columns2 =  model.table.columns.map((col)->
        if col.name == model.name then col.name + " as " + delta.new_name else col.name
    ).join(',')

    "-- (!!!) NOTE: this will delete your constraints and indexes\n"+
    "CREATE TEMPORARY TABLE #{tp_table}(#{columns_def});\n"+
    "INSERT INTO #{tp_table} SELECT #{columns2} FROM #{table};\n"+
    "DROP TABLE #{table};\n"+
    "CREATE TABLE #{table}(#{columns_def});\n"+
    "INSERT INTO #{table} SELECT #{columns} FROM #{tp_table};\n"+
    "DROP TABLE #{tp_table};"

  dropDatabase: (model)->
    "Not supported"

  dropTable: (model)->
    database = @escapeId(model.database.name)
    table = @escapeId(model.name)
    "DROP TABLE #{database}.#{table};"

  dropColumn: (model)->
    table = @escapeId(model.name)
    database = @escapeId(model.table.database.name)
    column = @escapeId(model.table.name)
    "ALTER TABLE #{database}.#{table} DROP COLUMN #{column};"

  sentenceReady: (callback)->
    @emitter.on 'sentence-ready', callback

  onDidChangeDefaultDatabase: (callback)->
    @emitter.on 'did-change-default-database', callback

  getDataTypes: ->
    @n_types.concat(@s_types)

  toString: ->
    @protocol+"://"+@info.file

  escapeConstant: (value)-> "'"+value.replace("'","''")+"'"

  escapeId: (value)-> '"'+value.replace('"','""')+'"'

  escape: (value,type)->
    for t1 in @s_types
      if value == null || type.search(new RegExp(t1, "i")) != -1
        return "'"+value+"'"
    value.toString()

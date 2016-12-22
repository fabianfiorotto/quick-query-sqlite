sql = require 'sql.js'
fs = require 'fs'

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
    @totalChanges = 0

  connect: (callback)->
    filebuffer = fs.readFileSync(@info.file);
    @db = new sql.Database(filebuffer)
    callback()

  serialize: ->
    file: @info.file
    protocol: @protocol

  dispose: ->
    @close()

  close: ->
    @db.close()

  query: (text,callback) ->
    message = null
    texts = @_splitStatments(text)
    try
      stmt = @db.prepare(texts[0])
      if stmt.isReadonly() && !@_isBegin(texts[0])
        rows = []
        fields = null
        while stmt.step()
          fields ?= stmt.getColumnNames().map (c)-> {name: c}
          rows.push stmt.get()
        if rows.length == 0
          fields = [{name: "No results"}]
        callback(message,rows,fields)
        stmt.free()
      else
        for text,i in texts
          stmt = @db.prepare(text) if i > 0
          stmt.step()
          stmt.free()
        result = @db.exec("SELECT total_changes() as totalChanges")
        if result[0]?.values[0][0]
          changes = result[0].values[0][0]
          if changes == @totalChanges
            message = {content: "Success"}
          else
            message = {content: "#{changes - @totalChanges} row(s) affected"}
          @totalChanges = changes
          @save() if @autosave
        else
          message = {content: "Success"}
        callback(message)
    catch e
      message = { type: 'error' , content: e.message }
      callback(message)

  objRowsMap: (rows,fields,callback)->
    rows.map (r,i) =>
      row = {}
      row[field.name] = r[j] for field,j in fields
      if callback? then callback(row) else row

  _isBegin: (str)->
    (/^(\s*\-\-.*\n)*\s*BEGIN.*$/i).test(str) #BEGIN TRANSACTION is readonly stmt

  _splitStatments: (str)->
    ii = []
    ch1 = null
    ch2 = null
    status = 1 #1 statment #2 comment #3 simple quote #4 double quote
    for i in [0..(str.length-1)]
      ch2 = ch1
      ch1 = str[i]
      switch status
        when 1
          if ch1 == ';' then ii.push(i+1)
          if ch1 == '-' && ch2 == '-' then status = 2
          if ch1 == "'" && ch2 != '\\' then status = 3
          if ch1 == '"' && ch2 != '\\' then status = 4
        when 2
          if ch1 == "\n" then status = 1
        when 3
          if ch1 == "'" && ch2 != '\\' then status = 1
        when 4
          if ch1 == '"' && ch2 != '\\' then status = 1
    strings = []
    i1 = 0
    for i in ii
      str1 = str.substring(i1,i)
      strings.push(str1) unless /^\s*$/.test(str1)
      i1 = i
    if i1 != str.length
      str1 = str.substring(i1,str.length)
      strings.push(str1) unless /^\s*$/.test(str1)
    return strings

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
    data = @db.export()
    buffer = new Buffer(data)
    fs.writeFileSync(@info.file, buffer);
    @totalChanges = 0 #saving resets the counter. I don't know why.

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

  alterColumn: (model,delta)->
    table_name = model.table.name
    tp_table = @escapeId(table_name+"_backup")
    table = @escapeId(table_name)
    text = "PRAGMA table_info('#{table_name}')"
    result = @db.exec(text)
    def_col = (col)=>
       if col[1] == model.name
        dafaultValue =  @escape(delta.default,delta.datatype)
        delta.new_name+" "+delta.datatype+
        (if col[5] == 1  then " PRIMARY KEY" else "") +
        (if !delta.nullable then " NOT NULL" else " NULL")+
        (if delta.default then " DEFAULT #{dafaultValue}" else "") #TODO default must be scaped
       else
        dafaultValue =  @escape(col[4],col[2])
        col[1]+" "+col[2] +
        (if col[5] == 1 then " PRIMARY KEY" else "") +
        (if col[3] == 1 then " NOT NULL" else " NULL")+
        (if col[4] then " DEFAULT #{@escape(col[4])}" else "")

    columns = result[0].values.map((col)->
       if col[1] == model.name then delta.new_name else col[1]
    ).join(',')
    columns_def = result[0].values.map(def_col).join(',')

    columns2 = result[0].values.map((col)->
        if col[1] == model.name then col[1]+" as "+delta.new_name else col[1]
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
    table_name = model.table.name
    tp_table = @escapeId(table_name+"_backup")
    table = @escapeId(table_name)
    text = "PRAGMA table_info('#{table_name}')"
    result = @db.exec(text)
    result[0].values = result[0].values.filter (col)-> col[1] != model.name
    def_col = (col)=>
      dafaultValue = @escape(col[4],col[2])
      col[1]+" "+ col[2] +
      (if col[5] == 1 then " PRIMARY KEY" else "") +
      (if col[3] == 1 then " NOT NULL" else " NULL")+
      (if col[4] then " DEFAULT #{dafaultValue}" else "")
    columns = result[0].values.map((col)-> col[1]).join(',')
    columns_def = result[0].values.map(def_col).join(',')
    "-- (!!!) NOTE: this will delete your constraints and indexes\n"+
    "CREATE TEMPORARY TABLE #{tp_table}(#{columns_def});\n"+
    "INSERT INTO #{tp_table} SELECT #{columns} FROM #{table};\n"+
    "DROP TABLE #{table};\n"+
    "CREATE TABLE #{table}(#{columns_def});\n"+
    "INSERT INTO #{table} SELECT #{columns} FROM #{tp_table};\n"+
    "DROP TABLE #{tp_table};"

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

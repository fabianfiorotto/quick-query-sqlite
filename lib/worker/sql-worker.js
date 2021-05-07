const initSqlJs = require('sql.js');
const fs = require('fs');

const SqlWorker = class SqlWorker {

  connect(info) {
    this.info = info;
    const filebuffer = fs.readFileSync(info.file);
    this.totalChanges = 0;
    return initSqlJs({
      locateFile: file => `../../node_modules/sql.js/dist/${file}`
    }).then((SQL) => {
      this.db = new SQL.Database(filebuffer);
      return false;
    });
  }

  query(text) {
    const texts = this._splitStatments(text);
    try {
      let stmt = this.db.prepare(texts[0])
      if (stmt.isReadonly() && !this._isBegin(texts[0]) ) {
        return this._readOnlyQuery(stmt);
      }
      else {
        return this._execMultiple(texts, stmt);
      }
    }
    catch (e) {
      console.error(e);
      return { type: 'error' , content: e.message }
    }
  }

  exec(text) {
    return this.db.exec(text)
  }

  save() {
    const data = this.db.export()
    const buffer = Buffer.from(data)
    fs.writeFileSync(this.info.file, buffer);
    this.totalChanges = 0 //saving resets the counter. I don't know why.
  }

  close() {
    this.db.close();
  }

  _readOnlyQuery(stmt) {
    const rows = [];
    let fields = null;

    while (stmt.step()) {
      if (fields == null) {
        fields = stmt.getColumnNames().map((c) => ({name: c}))
      }
      rows.push(stmt.get());
    }

    if (rows.length === 0) {
      fields = [{name: "No results"}];
    }

    stmt.free();
    return {rows, fields};
  }

  _execMultiple(texts, stmt) {
    for (let [i, text] of texts) {
      if (i > 0 || !stmt) {
        stmt = this.db.prepare(text);
      }
      stmt.step();
      stmt.free();
    }

    const result = this.db.exec("SELECT total_changes() as totalChanges");

    if (result[0] && result[0].values[0][0]) {
      const changes = result[0].values[0][0];
      if (changes !== this.totalChanges) {
        const latestChanges = changes - this.totalChanges;
        this.totalChanges = changes;
        return {
          type: 'success',
          content: `${latestChanges} row(s) affected`
        };
      }
    }
    return {
      type: 'success',
      content: "Success"
    };
  }

  _isBegin(str) {
    return (/^(\s*\-\-.*\n)*\s*BEGIN.*$/i).test(str);
  }

  _splitStatments(str) {
    const ii = [];
    let i, ch1 = null, ch2 = null;
    let status = 1; //1 statment #2 comment #3 simple quote #4 double quote

    for (i = 0; i < str.length-1; i++) {
      ch2 = ch1;
      ch1 = str[i];
      switch (status) {
        case 1:
          if (ch1 === ';') {
            ii.push(i + 1);
          }
          if (ch1 === '-' && ch2 === '-') {
            status = 2;
          }
          if (ch1 === "'" && ch2 !== '\\') {
            status = 3;
          }
          if (ch1 === '"' && ch2 !== '\\') {
            status = 4;
          }
          break;
        case 2:
          if (ch1 === "\n") {
            status = 1;
          }
          break;
        case 3:
          if (ch1 === "'" && ch2 !== '\\') {
            status = 1;
          }
          break;
        case 4:
          if (ch1 === '"' && ch2 !== '\\') {
            status = 1;
          }
      }
    }
    const strings = [];
    let str1, i1 = 0;

    for (i in ii) {
      str1 = str.substring(i1, i);
      if (!/^\s*$/.test(str1)) {
        strings.push(str1);
      }
      i1 = i;
    }
    if (i1 !== str.length) {
      str1 = str.substring(i1, str.length);
      if (!/^\s*$/.test(str1)) {
        strings.push(str1);
      }
    }

    return strings;
  }

}


//     --- WORKER ---

const sqlWorker = new SqlWorker();

onmessage = function (oEvent) {
  const {id, action, params} = oEvent.data;
  const res = sqlWorker[action](...params);
  Promise.resolve(res).then (response => {
    postMessage({id, response});
  })
};

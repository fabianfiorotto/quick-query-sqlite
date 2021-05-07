
module.exports = class SqlProxy

  @createConnection: (info, cb) ->
    instance = new SqlProxy()
    instance.connect(info, cb);
    return instance;

  listen: (oEvent)->
    {id, response} = oEvent.data
    index = @callbacks.findIndex((callback) -> callback.id == id)
    @callbacks[index].cb?(response)
    @callbacks.splice(index, 1)

  connect: (info, cb) ->
    @worker = new Worker(require.resolve("./worker/sql-worker"));
    @worker.addEventListener("message", ((oEvent) => @listen(event)), false);
    @callbacks = [];
    @last_id = 0;
    @sendAction("connect",[info],cb)

  query: (text, cb)->
    @sendAction("query",[text],cb)

  exec: (text, cb)->
    @sendAction("exec",[text],cb)

  save: (cb)->
    @sendAction("save",[],cb)

  close: (cb) ->
    @sendAction "close",[], =>
      @worker.terminate()
      cb?()

  sendAction: (action, params, cb)->
    id = @last_id++
    @worker.postMessage({id, action, params});
    callback = {cb, id}
    @callbacks.push(callback)

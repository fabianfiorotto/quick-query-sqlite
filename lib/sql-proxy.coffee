
module.exports = class SqlProxy

  @createConnection: (info, cb) ->
    instance = new SqlProxy()
    instance.connect(info).then(cb);
    return new Proxy instance,
        get: (target, key) ->
          target[key] || ((args...) -> target.sendAction(key, args))

  listen: (oEvent)->
    {id, response, error} = oEvent.data
    index = @callbacks.findIndex((callback) -> callback.id == id)
    if error?
      @callbacks[index].reject(error)
    else
      @callbacks[index].resolve(response)
    @callbacks.splice(index, 1)

  connect: (info) ->
    @worker = new Worker(require.resolve("./worker/sql-worker"));
    @worker.addEventListener("message", ((oEvent) => @listen(event)), false);
    @callbacks = [];
    @last_id = 0;
    @sendAction("connect",[info])

  close: ->
    @sendAction("close",[]).then =>
      @worker.terminate()

  sendAction: (action, params)->
    new Promise (resolve, reject) =>
      id = @last_id++
      @worker.postMessage({id, action, params});
      callback = {resolve, reject, id}
      @callbacks.push(callback)

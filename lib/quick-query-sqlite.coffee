QuickQuerySqliteConnection = require './quick-query-sqlite-connection'

{CompositeDisposable} = require 'atom'

module.exports = QuickQuerySqlite =
  subscriptions: null

  activate: (state) ->
    @subscriptions = new CompositeDisposable
    atom.commands.add '.quick-query-browser',
      'quick-query-sqlite:save', =>
         @browserView.selectedConnection.save() if @browserView

  deactivate: ->
    @subscriptions.dispose()

  serialize: ->

  consumeBrowserView: (browserView)->
    @browserView = browserView

  consumeConnectView: (connectView)->
    protocol =
      name: 'Sqlite'
      handler: QuickQuerySqliteConnection
    connectView.addProtocol('sqlite',protocol)
    protocol =
      name: 'Sqlite (autosave)'
      handler: QuickQuerySqliteConnection
      default:
        autosave: true
    connectView.addProtocol('sqlite-autosave',protocol)

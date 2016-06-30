QuickQuerySqliteConnection = require './quick-query-sqlite-connection'

{CompositeDisposable} = require 'atom'

module.exports = QuickQuerySqlite =
  subscriptions: null

  activate: (state) ->
    @subscriptions = new CompositeDisposable
    atom.commands.add '.quick-query-browser',
      'quick-query-sqlite:save', =>
         @browserView.selectedConnection.save() if @browserView

    @subscriptions.add atom.workspace.addOpener (uriToOpen) =>
      return unless ///\.(#{QuickQuerySqliteConnection.fileExtencions.join('|')})$///.test uriToOpen
      if !@browserView || !@browserView.is(':visible')
        workspaceElement = atom.views.getView(atom.workspace)
        atom.commands.dispatch workspaceElement, 'quick-query:toggle-browser'
      #Don't open a connection twice. justo ensure that the connection is selected.
      connection = (i for i in @browserView.connections when i.protocol is 'sqlite' && i.info.file == uriToOpen)[0]
      if !connection
        connectionPromise = @connectView.buildConnection({protocol: 'sqlite', autosave: false, file: uriToOpen})
        @browserView.addConnection(connectionPromise)
      else if @browserView.selectedConnection != connection
        @browserView.selectedConnection = connection
        @browserView.trigger('quickQuery.connectionSelected',[connection])
        @browserView.showConnections()
      return true

  deactivate: ->
    @subscriptions.dispose()

  serialize: ->

  consumeBrowserView: (browserView)->
    @browserView = browserView

  consumeConnectView: (connectView)->
    @connectView = connectView
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

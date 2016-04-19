
require 'hanami/model/adapters/abstract'
require 'hanami/model/adapters/implementation'
require 'hanami/model/adapters/rethinkdb/collection'
require 'hanami/model/adapters/rethinkdb/coercer'
require 'hanami/model/adapters/rethinkdb/command'
require 'hanami/model/adapters/rethinkdb/query'
require 'pry'

require 'rethinkdb'

module Hanami
  module Model
    module Adapters
      class RethinkDBAdapter < Abstract
        include ::RethinkDB::Shortcuts
        include Implementation

        def initialize(mapper, uri, options = {})
          super
          @uri            = URI.parse(@uri)
          host, port, db  = [@uri.host, @uri.port, @uri.path.gsub('/', '')]

          @connection = r.connect(host: host, port: port, db: db)
          options[:after_connect].call if options[:after_connect]
        rescue URI::InvalidURIError => e
          raise e
        rescue Exception => e
          raise DatabaseAdapterNotFound.new(e.message)
        end

        # Creates a record in the database for the given entity.
        # It assigns the `id` attribute, in case of success.
        #
        # @param collection [Symbol] the target collection (it must be mapped).
        # @param entity [#id=] the entity to create
        #
        # @return [Object] the entity
        #
        # @api private
        # @since 0.1.0
        def create(collection, entity)
          command(
            query(collection)
          ).create(entity)
        end

        # Updates a record in the database corresponding to the given entity.
        #
        # @param collection [Symbol] the target collection (it must be mapped).
        # @param entity [#id] the entity to update
        #
        # @return [Object] the entity
        #
        # @api private
        # @since 0.1.0
        def update(collection, entity)
          command(
            _find(collection, entity.id)
          ).update(entity)
        end

        # Deletes a record in the database corresponding to the given entity.
        #
        # @param collection [Symbol] the target collection (it must be mapped).
        # @param entity [#id] the entity to delete
        #
        # @api private
        # @since 0.1.0
        def delete(collection, entity)
          command(
            _find(collection, entity.id)
          ).delete
        end

        # Deletes all the records from the given collection.
        #
        # @param collection [Symbol] the target collection (it must be mapped).
        #
        # @api private
        # @since 0.1.0
        def clear(collection)
          command(query(collection)).clear
        end

        # Fabricates a command for the given query.
        #
        # @param query [Hanami::Model::Adapters::RethinkDB::Query] the query object to
        #   act on.
        #
        # @return [Hanami::Model::Adapters::RethinkDB::Command]
        #
        # @see Hanami::Model::Adapters::RethinkDB::Command
        #
        # @api private
        # @since 0.1.0
        def command(query)
          RethinkDB::Command.new(query, @connection)
        end

        # Fabricates a query
        #
        # @param collection [Symbol] the target collection (it must be mapped).
        # @param blk [Proc] a block of code to be executed in the context of
        #   the query.
        #
        # @return [Hanami::Model::Adapters::RethinkDB::Query]
        #
        # @see Hanami::Model::Adapters::RethinkDB::Query
        #
        # @api private
        # @since 0.1.0
        def query(collection, context = nil, &blk)
          RethinkDB::Query.new(_collection(collection), @connection, context, &blk)
        end

        # Wraps the given block in a transaction.
        #
        # For performance reasons the block isn't in the signature of the method,
        # but it's yielded at the lower level.
        #
        # @param options [Hash] options for transaction
        # @option rollback [Symbol] the optional rollback policy: `:always` or
        #   `:reraise`.
        #
        # @see Hanami::Repository::ClassMethods#transaction
        #
        # @since 0.2.3
        # @api private
        #
        # @example Basic usage
        #   require 'hanami/model'
        #
        #   class Article
        #     include Hanami::Entity
        #     attributes :title, :body
        #   end
        #
        #   class ArticleRepository
        #     include Hanami::Repository
        #   end
        #
        #   article = Article.new(title: 'Introducing transactions',
        #     body: 'lorem ipsum')
        #
        #   ArticleRepository.new.transaction do
        #     ArticleRepository.dangerous_operation!(article) # => RuntimeError
        #     # !!! ROLLBACK !!!
        #   end
        #
        # @example Policy rollback always
        #   require 'hanami/model'
        #
        #   class Article
        #     include Hanami::Entity
        #     attributes :title, :body
        #   end
        #
        #   class ArticleRepository
        #     include Hanami::Repository
        #   end
        #
        #   article = Article.new(title: 'Introducing transactions',
        #     body: 'lorem ipsum')
        #
        #   ArticleRepository.new.transaction(rollback: :always) do
        #     ArticleRepository.new.create(article)
        #     # !!! ROLLBACK !!!
        #   end
        #
        #   # The operation is rolled back, even in no exceptions were raised.
        #
        # @example Policy rollback reraise
        #   require 'hanami/model'
        #
        #   class Article
        #     include Hanami::Entity
        #     attributes :title, :body
        #   end
        #
        #   class ArticleRepository
        #     include Hanami::Repository
        #   end
        #
        #   article = Article.new(title: 'Introducing transactions',
        #     body: 'lorem ipsum')
        #
        #   ArticleRepository.new.transaction(rollback: :reraise) do
        #     ArticleRepository.dangerous_operation!(article) # => RuntimeError
        #     # !!! ROLLBACK !!!
        #   end # => RuntimeError
        #
        #   # The operation is rolled back, but RuntimeError is re-raised.
        def transaction(options = {})
          @connection.transaction(options) do
            yield
          end
        end

        # Returns a string which can be executed to start a console suitable
        # for the configured database, adding the necessary CLI flags, such as
        # url, password, port number etc.
        #
        # @return [String]
        #
        # @since 0.3.0
        def connection_string
          RethinkDB::Console.new(@uri).connection_string
        end

        # Executes a raw SQL command
        #
        # @param raw [String] the raw SQL statement to execute on the connection
        #
        # @raise [Hanami::Model::InvalidCommandError] if the raw SQL statement is invalid
        #
        # @return [NilClass]
        #
        # @since 0.3.1
        def execute(raw)
          begin
            @connection.execute(raw)
            nil
          rescue Sequel::DatabaseError => e
            raise Hanami::Model::InvalidCommandError.new(e.message)
          end
        end

        # Fetches raw result sets for the given SQL query
        #
        # @param raw [String] the raw SQL query
        # @param blk [Proc] optional block that is yielded for each record
        #
        # @return [Array]
        #
        # @raise [Hanami::Model::InvalidQueryError] if the raw SQL statement is invalid
        #
        # @since 0.5.0
        def fetch(raw, &blk)
          if block_given?
            @connection.fetch(raw, &blk)
          else
            @connection.fetch(raw).to_a
          end
        rescue Sequel::DatabaseError => e
          raise Hanami::Model::InvalidQueryError.new(e.message)
        end

        # @api private
        # @since 0.5.0
        #
        # @see Hanami::Model::Adapters::Abstract#disconnect
        def disconnect
          @connection.disconnect
          @connection = DisconnectedResource.new
        end

        private

        # Returns a collection from the given name.
        #
        # @param name [Symbol] a name of the collection (it must be mapped).
        #
        # @return [Hanami::Model::Adapters::RethinkDB::Collection]
        #
        # @see Hanami::Model::Adapters::RethinkDB::Collection
        #
        # @api private
        # @since 0.1.0
        def _collection(name)
          mapped = _mapped_collection(name) # implementation
          RethinkDB::Collection.new(r.table(name), mapped, @connection)
        end

      end
    end
  end
end

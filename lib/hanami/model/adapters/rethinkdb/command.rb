module Hanami
  module Model
    module Adapters
      module RethinkDB
        # Execute a command for the given query.
        #
        # @see Hanami::Model::Adapters::RethinkDB::Query
        #
        # @api private
        # @since 0.1.0
        class Command
          # @api private
          # @since 0.6.1
          SEQUEL_TO_HANAMI_ERROR_MAPPING = {
            'Sequel::UniqueConstraintViolation'     => Hanami::Model::UniqueConstraintViolationError,
            'Sequel::ForeignKeyConstraintViolation' => Hanami::Model::ForeignKeyConstraintViolationError,
            'Sequel::NotNullConstraintViolation'    => Hanami::Model::NotNullConstraintViolationError,
            'Sequel::CheckConstraintViolation'      => Hanami::Model::CheckConstraintViolationError
          }.freeze

          # Initialize a command
          #
          # @param query [Hanami::Model::Adapters::RethinkDB::Query]
          #
          # @api private
          # @since 0.1.0
          def initialize(query, connection)
            @collection = query.scoped
            @connection = connection
          end

          # Creates a record for the given entity.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Hanami::Model::Adapters::RethinkDB::Collection#insert
          #
          # @return the primary key of the just created record.
          #
          # @api private
          # @since 0.1.0
          def create(entity)
            _handle_database_error {
              @collection.insert(entity)
            }
          end

          # Updates the corresponding record for the given entity.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Hanami::Model::Adapters::RethinkDB::Collection#update
          #
          # @api private
          # @since 0.1.0
          def update(entity)
            _handle_database_error {
              @collection.update(entity)
            }
          end

          # Deletes all the records for the current query.
          #
          # It's used to delete a single record or an entire database table.
          #
          # @see Hanami::Model::Adapters::RethinkDBAdapter#delete
          # @see Hanami::Model::Adapters::RethinkDBAdapter#clear
          #
          # @api private
          # @since 0.1.0
          def delete
            _handle_database_error {
              @collection.delete.run(@connection)
            }
          end

          alias_method :clear, :delete

          private

          # Handles any possible Adapter's Database Error
          #
          # @api private
          # @since 0.6.1
          def _handle_database_error
            yield
          rescue Sequel::DatabaseError => e
            error_class = SEQUEL_TO_HANAMI_ERROR_MAPPING.fetch(e.class.name, Hanami::Model::InvalidCommandError)
            raise error_class, e.message
          end
        end
      end
    end
  end
end

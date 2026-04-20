class AddJobCreationFailures < ActiveRecord::Migration[4.2]
  def up

    #add failure type
    execute("CREATE TYPE ds3.job_creation_failed_type AS ENUM ()")
    execute("ALTER TYPE ds3.job_creation_failed_type add value if not exists 'TAPES_MUST_BE_ONLINED'")

    #create failure table
    execute("CREATE TABLE ds3.job_creation_failed (
      id               uuid                         NOT NULL,
      date             timestamp without time zone  NOT NULL,
      error_message    varchar                      NULL,
      tape_bar_codes   varchar                      NULL,
      user_name        varchar                      NOT NULL,
      type             ds3.job_creation_failed_type NOT NUlL,
      PRIMARY KEY (id)
    );")
    execute("CREATE INDEX ON ds3.job_creation_failed (date)")
  end

  def down
    execute("DROP table ds3.job_creation_failed;")
    execute("drop type ds3.job_creation_failed_type;")
  end
end

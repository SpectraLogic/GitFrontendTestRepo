1. Make sure you have Docker installed and running on your machine.
2. Check if there are any container containers running on ports 5432 and 5433. If there are, stop them.
      You can do this by running the following command:
        ```
        docker ps
        ```
3. Before running the tests, make sure all docker volumes and containers are cleaned up.
    You can do this by running the following command:
    ```
    docker system prune -a
    docker volume prune -f
    ```
4. Build the code by running the following command:
    ```
    packageAll.sh
    ```
4. To run the tests, navigate to the `frontend` directory and execute the following command:
    ```
    docker-compose -f dbmigrationtest/docker-compose-testdb.yml run --rm databasetest bundle exec ruby compare_databases.rb dao.sql Rakefile
    ```

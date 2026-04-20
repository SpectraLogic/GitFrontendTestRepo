Steps to run integration tests:
Setup:
	- Make sure docker is running. (Run `docker info`)
		ashapillai@alex-precision7560 frontend % docker info
		Client: Docker Engine - Community
		 Version:    28.1.1
		 Context:    colima

	- Steps to set up docker (MAC/Linux)
	    Follow instructions : https://gist.github.com/juancsr/5927e6660d6ba5d2a34c61802d26e50a

	- Before running docker tests, make sure you have cleared docker volumes and containers.
		docker system prune -a
		docker volume prune

	- To tweak, memory and clock issues while running tests, do the following steps before you run the tests:
		○ colima ssh
		○ sudo apt update
		○ sudo apt install ntpdate
		○ sudo ntpdate pool.ntp.org
		○ colima stop
		○ colima start --memory 4 --cpu 4

	- If you want to run the full test suite: run ./runIntegrationTests.sh.
    - If you want to run individual tests, go to docker, run `docker-compose up`. This will bring up all docker containers. You can go to IntelliJ and run the tests.


To test locally testAll.sh (with same env as nexus), env variables used are:
SKIP_DOCKER_INTEGRATION -- this will skip docker integration tests
SKIP_RPC_TESTS -- this will skip tests with tag(rpc-integration)


- We have the ability to run individual tests against real BP. To do that, you need to set the following env variables:
  BP_USED=true
  DS3_ENDPOINT=<datapath endpoint of BP>
  DS3_ACCESS_KEY=<>
  DS3_SECRET_KEY=<>
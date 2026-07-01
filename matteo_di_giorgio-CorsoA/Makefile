CC	=  gcc
CFLAGS  = -pthread   

DEBUG 	= -DDEBUG

TESTFILE = test1.txt test2.txt

.PHONY: test1 test2 clean

all: ./supermarket

./supermarket: ./supermarket.c
	$(CC) $(CFLAGS) -I./ -L./ -o sup.out ./supermercato.c

./director: ./director.c
	$(CC) $(CFLAGS) -I./ -L./ -o dir.out ./direttore.c


test1:
	(./dir.out & echo $! > direttore.PID) &
	(./sup.out test1.txt & echo $! > supermercato.PID) &
	sleep 15s; \
	kill -s SIGQUIT $(cat direttore.PID) \
	chmod +x ./reader.sh 
	./reader.sh $(cat supermercato.PID) \

test2:
	(./dir.out & echo $! > direttore.PID) &
	(./sup.out test2.txt & echo $! > supermercato.PID) &
	sleep 25s; \
	kill -s SIGQHUP $(cat direttore.PID) \
	chmod +x ./reader.sh 
	./reader.sh $(cat supermercato.PID) \
	
clean:
	rm -f ./statsfile.log socket $(cat supermercato.PID) $(cat direttore.PID) 
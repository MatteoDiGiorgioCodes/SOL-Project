#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include <time.h>
#include <signal.h>
#include <math.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h> /* ind AF_UNIX */
#include <conn.h>
#define SIGHUP 1
#define SIGQUIT 3

#define UNIX_PATH_MAX 108 /* man 7 unix */
int fd_sign;

typedef struct threadArgs 
{
	int socket;
} threadArgs_t;

void *Perm(void *arg)
{
	int my_sock = ((threadArgs_t*)arg)->socket;
	//va in attesa che qualcuno scriva 1, lui risponde 1
	int q,a=1;
	
	while(1)	
	{
		readn(my_sock,&q,sizeof(int));
		if(q!=-1)
			writen(my_sock,&a,sizeof(int));
		else break;
	}
	
	close(my_sock);
	return NULL;
}


static void gestore_sighup (int signum)
{
    int a=0; 
	writen(fd_sign,&a,sizeof(int));//stampa 0 sull'apposito file
}

static void gestore_sigquit (int signum)
{
    int a=1;
	writen(fd_sign,&a,sizeof(int));//stampa 1 sull'apposito file
}


int main()
{		
	
	int fd_skt, fd_c, fd_main, fd_perm, i, new_state, K, distanza_tra_controlli, S1, S2, supp; 
	int num_casse_troppi_clienti=0, num_casse_pochi_clienti=0, num=0;
	int buf[3];
	struct sockaddr_un sa;
	struct sigaction sighup_hand, sigquit_hand;
	
    sighup_hand.sa_handler = gestore_sighup;
    sigemptyset(&sighup_hand.sa_mask);
    sighup_hand.sa_flags = 0;
    
    if (sigaction(SIGHUP, &sighup_hand, NULL) == -1)
        fprintf(stderr, "SIGQUIT handler allocation failed\n");;
    
    sigquit_hand.sa_handler = gestore_sigquit;
    sigemptyset(&sigquit_hand.sa_mask);
    sigquit_hand.sa_flags = 0;
    
    if (sigaction(SIGQUIT, &sigquit_hand, NULL) == -1)
        fprintf(stderr, "SIGQUIT handler allocation failed\n");
        
	pthread_t    *permits;
    threadArgs_t *perARGS;
	
	permits = malloc(sizeof(pthread_t));
    perARGS = malloc(sizeof(threadArgs_t));
    
    if (!permits || !perARGS) 
	{
        fprintf(stderr, "malloc fallita\n");
        exit(EXIT_FAILURE);
    }

	strncpy(sa.sun_path, SOCKNAME,UNIX_PATH_MAX);
	sa.sun_family=AF_UNIX;
	fd_skt=socket(AF_UNIX,SOCK_STREAM,0);
	bind(fd_skt,(struct sockaddr *)&sa,sizeof(sa));
	listen(fd_skt,SOMAXCONN);
	for(i=0;i<3;i++)
	{
		fd_c=accept(fd_skt,NULL,0);		
		while(fd_c==-1)
			fd_c=accept(fd_skt,NULL,0);	
		readn(fd_c,&supp,sizeof(int));
		if(supp==3)
			fd_main=fd_c;
		else if(supp==1)
			fd_perm=fd_c;
		else fd_sign=fd_c;
	}
    
    perARGS->socket=fd_perm;
    
    if (pthread_create(permits, NULL, Perm, perARGS) != 0)
    {
        fprintf(stderr, "pthread_create failed (Perm)\n");
    	exit(EXIT_FAILURE);
    }
    
	
	for(i=0;i<3;i++)
		readn(fd_main,&buf[i],sizeof(int));	
	K=buf[0];
	S1=buf[1];
	S2=buf[2];
	
	int casse[K][2];
	
	while(1)
	{
	
		num_casse_troppi_clienti=0;
    		num_casse_pochi_clienti=0;
		for(i=0;i<K;i++)
		{
			readn(fd_main,&casse[i][0],sizeof(int));
			if(casse[i][0]==-1)
				break;
			readn(fd_main,&casse[i][1],sizeof(int));
			if(casse[i][1]>S2&&casse[i][0]==1)
            			num_casse_troppi_clienti++;
        		else if(casse[i][1]<2&&casse[i][0]==1)
            			num_casse_pochi_clienti++;
		}
		if(casse[i][0]==-1)
			break;
		num=num_casse_troppi_clienti-(num_casse_pochi_clienti/S1);
		i=0;
	
		while(i<K&&num!=0)
    		{
			if(casse[i][1]==0&&num>0)
        		{
           			new_state=0;
				writen(fd_main,&i,sizeof(int));
            			writen(fd_main,&new_state,sizeof(int));
				num--;
        		}
        		else if(casse[i][1]==1&&num<0)
        		{
            			new_state=1;
				writen(fd_main,&i,sizeof(int));
            			writen(fd_main,&new_state,sizeof(int));
            			num++;
    			}
        		i++;
    		}
    		num=-1;
    		writen(fd_main,&num,sizeof(int));
	}
	num=-2;
	writen(fd_main,&num,sizeof(int));
	pthread_join(*permits,NULL);
	close(fd_skt);
	close(fd_main);
	close(fd_sign);
	unlink(SOCKNAME);
	free(permits);
	free(perARGS);
	printf("direttore termina\n");
    return 0;
}

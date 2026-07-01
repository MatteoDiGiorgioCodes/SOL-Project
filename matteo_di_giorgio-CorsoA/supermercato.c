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
#include <sys/un.h>
#include <conn.h>

#define SIGHUP 1
#define SIGQUIT 3
#define MAX_LEN 256

struct client_node
{
	struct client_node* next;
	struct client_node* pred;
	int id;
	int num_pred;
	int nprod;
	int finito;
	int in_servizio;
	int myid;
	pthread_mutex_t nlock;
};

struct cassa
{
	struct timespec tempo_extra_t;
	struct timespec tempo_per_prodotto_t;
	struct client_node* testa;
	struct client_node* coda;
	int lunghezza;
	int aperta;
	int chiuditi;
	int chiusure;
	float tempo_apertura_t;
	struct timespec apri_t;
	struct timespec chiudi_t;
	pthread_mutex_t qlock;
	pthread_cond_t qcond;
	int myid;
};

typedef struct accesso_entrate 
{
	int* e;
	int num_usciti;
	pthread_mutex_t lock;
	pthread_cond_t cond;
} accesso;

typedef struct uscita_senza_prodotti
{
	pthread_mutex_t richieste;
	pthread_mutex_t lock;
	pthread_cond_t cond;
}no_prod;

typedef struct threadArgs
{
	int      thid;
	accesso *acc;
	no_prod *no_p;
	int         index;
	struct cassa *q;
	int tempo_extra;
	int tempo_per_prodotto;
	FILE *f;
} threadArgs_t;

static inline void LockArray(accesso *q)            { pthread_mutex_lock(&q->lock);   }
static inline void UnlockArray(accesso *q)          { pthread_mutex_unlock(&q->lock); }
static inline void UnlockArrayAndWait(accesso *q)   { pthread_cond_wait(&q->cond, &q->lock); }
static inline void UnlockArrayAndSignal(accesso *q) { pthread_cond_signal(&q->cond); pthread_mutex_unlock(&q->lock);}

static inline void LockReq(no_prod *q)            { pthread_mutex_lock(&q->richieste);   }
static inline void UnlockReq(no_prod *q)          { pthread_mutex_unlock(&q->richieste); }
static inline void LockNoProd(no_prod *q)         { pthread_mutex_lock(&q->lock);   }
static inline void UnlockNoProd(no_prod *q)       { pthread_mutex_unlock(&q->lock); }
static inline void SendASignal(no_prod *q)   	  { pthread_cond_signal(&q->cond);}
static inline void WaitForSignal(no_prod *q)   	  { pthread_cond_wait(&q->cond, &q->lock); }

static inline void LockNode(struct client_node *q)            { pthread_mutex_lock(&q->nlock);   }
static inline void UnlockNode(struct client_node *q)          { pthread_mutex_unlock(&q->nlock); }

static inline void LockQueue(struct cassa *q)            { pthread_mutex_lock(&q->qlock);   }
static inline void UnlockQueue(struct cassa *q)          { pthread_mutex_unlock(&q->qlock); }
static inline void UnlockQueueAndWait(struct cassa *q)   { pthread_cond_wait(&q->qcond, &q->qlock); }
static inline void UnlockQueueAndSignal(struct cassa *q) { pthread_cond_signal(&q->qcond); pthread_mutex_unlock(&q->qlock);} //a cosa serve qcond???

pthread_mutex_t filemutex;

int K;
int C;
int E;
struct timespec T;
int P;
int S;
int S1;
int S2;
int distanza_tra_controlli;

int porte=1;
int supermercato=1;
int clienti=1;
int no_products=0;

void converti_tempo(int ms, struct timespec *time)
{
	time->tv_nsec=(ms%1000)*1000000;
	time->tv_sec=ms/1000;	
}

void AggiungiUscito(struct client_node* node, accesso* p)
{
	LockArray(p);
	*((p->e)+(p->num_usciti))=node->id;
	free(node);
	p->num_usciti++;
	if(p->num_usciti==E)
		UnlockArrayAndSignal(p);
	else UnlockArray(p);
}

int push(struct cassa *q, struct client_node *n) 
{
	if ((q == NULL) || (n == NULL))
		return -1;
	LockQueue(q); 
	if(q->lunghezza!=0)
	{
		q->coda->next=n;	
		n->pred=q->coda;	
	}	
	else q->testa=n;
	q->coda = n;
	n->num_pred=q->lunghezza;
	q->lunghezza++;
	UnlockQueueAndSignal(q);   
	return 0;
}

void* pop(struct cassa* q, accesso* p) 
{
    if (q == NULL)
        return NULL;
    LockQueue(q);    
    struct client_node *data  = q->testa;
    q->testa = q->testa->next;
	if(q->testa!=NULL)
    	q->testa->pred=NULL;
    q->lunghezza--;
    data->in_servizio=1;
	UnlockQueue(q);
    return data;
}

void *Attesa(void *arg)
{
	FILE *f = ((threadArgs_t*)arg)->f;	
	struct sockaddr_un serv_addr;
	int sockfd;
	int signal;
	int code=2;
    
	sockfd=socket(AF_UNIX, SOCK_STREAM, 0);
	memset(&serv_addr, '0', sizeof(serv_addr));
	serv_addr.sun_family = AF_UNIX;    
	strncpy(serv_addr.sun_path,SOCKNAME, strlen(SOCKNAME)+1);
	while (connect(sockfd,(struct sockaddr*)&serv_addr,sizeof(serv_addr)) == -1 ) 
	{
		if ( errno == ENOENT )
			sleep(1); /* sock non esiste */
		else exit(EXIT_FAILURE);
	}
	
	writen(sockfd, &code, sizeof(int));
	printf("attesa segnali connesso\n");
	readn(sockfd, &signal, sizeof(int));
    
	if(signal==0)
    	porte=0;
	else if(signal==1)
	{		
		supermercato=0;
		porte=0;
	}
	close(sockfd);
	return NULL;
}


void *Uscita_No_Prodotti(void *arg)
{
	no_prod* a = ((threadArgs_t*)arg)->no_p;
	struct sockaddr_un serv_addr;
	int sockfd;
	int qu=1;
	int an;
	int code=1;
	sockfd=socket(AF_UNIX, SOCK_STREAM, 0);
	memset(&serv_addr, '0', sizeof(serv_addr));
		serv_addr.sun_family = AF_UNIX;    
	strncpy(serv_addr.sun_path,SOCKNAME, strlen(SOCKNAME)+1);
	while (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1 ) 
	{
		if ( errno == ENOENT )
			sleep(1); /* sock non esiste */
		else exit(EXIT_FAILURE); 
	}
	writen(sockfd, &code, sizeof(int));
	printf("uscita senza prodotti connesso\n");	
	LockNoProd(a);
	no_products=1;
	while(no_products==1)
	{		
		WaitForSignal(a);
		writen(sockfd, &qu, sizeof(int));
		readn(sockfd, &an, sizeof(int));
		if(an==1)
			SendASignal(a);			
	}
	UnlockNoProd(a);
	qu=-1;
	writen(sockfd, &qu, sizeof(int));
	close(sockfd);
	return NULL;
}

void *Cliente(void *arg)
{
	int myid = ((threadArgs_t*)arg)->thid;
	int myindex = ((threadArgs_t*)arg)->index;
	struct cassa *q = ((threadArgs_t*)arg)->q;
	accesso* a = ((threadArgs_t*)arg)->acc;
	no_prod* np = ((threadArgs_t*)arg)->no_p;
	FILE *f = ((threadArgs_t*)arg)->f;
	float b;
	float in;
	int   ncoda;
	int msec;
	int new_msec;
	struct client_node* node;
	int numprod;
	float tim;
	struct timespec support;
	struct timespec time_in;
	struct timespec time_queue_before;
	struct timespec time_queue_after;
	int codevisitate=0;
	int i;
	int int_support;
	int newqueue=-1;
	unsigned int seed;
    
	clock_gettime(CLOCK_MONOTONIC, &support);
	seed=support.tv_nsec/(myid+1);
	numprod=(rand_r(&seed)%(P+1));
    
	if(T.tv_sec!=0)
		time_in.tv_sec=(time_t)(rand_r(&seed)%((int)T.tv_sec));
	else time_in.tv_sec=0;
	if(T.tv_nsec!=0)
	{
		time_in.tv_nsec=(long)rand_r(&seed)%(((int)T.tv_nsec/10000000)-1)+1;
		time_in.tv_nsec=time_in.tv_nsec*10000000;
	}	
	else time_in.tv_nsec=0;
	
	nanosleep(&time_in, NULL);
    
	node = malloc(sizeof(struct client_node));
        
	if (pthread_mutex_init(&(node->nlock), NULL) != 0)
	{
		perror("mutex init");
		return NULL;
   	}
		
	node->myid=myid;
	node->pred=NULL;   
	node->next=NULL;    
	node->nprod=numprod;    
	node->finito=0;   
	node->in_servizio=0;    
	node->id=myindex;
    
	if(numprod!=0)
	{
		ncoda=rand_r(&seed)%K;
 
		while((q+ncoda)->aperta==0&&supermercato==1)
 		{
			ncoda=(ncoda+1)%K;
		}

		if(supermercato==1)
		{
			push((q+ncoda), node);
			clock_gettime(CLOCK_MONOTONIC,&time_queue_before);
			codevisitate++;
		}
		else b=0;//ossia non mi sono mai messo in coda
		
		clock_gettime(CLOCK_MONOTONIC, &support);
		msec=(support.tv_sec*1000)+(support.tv_nsec/1000000);
    
		while(supermercato==1)
		{
			
			LockNode(node);
			
			int_support=node->num_pred;
			newqueue=-1;
			clock_gettime(CLOCK_MONOTONIC, &support);
			new_msec=(support.tv_sec*1000)+(support.tv_nsec/1000000);
			if(new_msec>msec+S)
			{
			
				if(node->pred!=NULL)
					node->num_pred=node->pred->num_pred+1;
				else node->num_pred=0;
			
				for(i=0;i<K;i++)
				{	if(i!=ncoda)
						if((q+i)->lunghezza<int_support&&(q+i)->aperta==1)
						{
							int_support=(q+i)->lunghezza;
							newqueue=i;
						}
				} 
				msec=new_msec;
			}
					
			if(((q+ncoda)->aperta==0&&node->in_servizio==0&&node->finito==0)||(newqueue!=-1&&node->in_servizio==0&&node->finito==0))
			{
				LockQueue(q+ncoda);    
				if(node->pred!=NULL)
					node->pred->next=node->next;
				else (q+ncoda)->testa=node->next;
				
				if(node->next!=NULL)
					node->next->pred=node->pred;
				else (q+ncoda)->coda=node->pred;
				
				node->pred=NULL;
				node->next=NULL;
				
				if((q+ncoda)->lunghezza>0)
					(q+ncoda)->lunghezza--;
					
				UnlockQueue(q+ncoda);
				
				if(newqueue==-1)
				{
					ncoda=rand_r(&seed)%K;
					i=0;
                			while((q+ncoda)->aperta==0)
                			{
						ncoda=(ncoda+1)%K;
						i++;
						if(i>K)
						break;
						    
                			}
                			if(i>K)
					{
						UnlockNode(node);
						break;
					}
				}	
				else ncoda=newqueue;

                		push((q+ncoda), node); 
                
                		codevisitate++;
                		UnlockNode(node);
            		}
            		else if(node->finito==1)
			{
            			UnlockNode(node);
				break;	
			}
			else 
			{
            			UnlockNode(node);
			}    
        	}
        	clock_gettime(CLOCK_MONOTONIC,&time_queue_after);
    	}
    	else
	{
		while(no_products==0)// pessima implementazione di attesa, ma usare una variabile di condizione per un'attesa che verrà effettuata così poco mi sembrava un'idea peggiore
		{
		}
		LockReq(np);
		LockNoProd(np);
		SendASignal(np);
		WaitForSignal(np);
		UnlockNoProd(np);
		UnlockReq(np);
		b=0;	
	} 
	
	LockNode(node);
	in=time_in.tv_sec+((float)time_in.tv_nsec/1000000000);
	if(codevisitate!=0)
		b=(time_queue_after.tv_sec-time_queue_before.tv_sec)+((float)(time_queue_after.tv_nsec-time_queue_before.tv_nsec)/1000000000);	
	
	if(node->finito==0)
		numprod=0;
	UnlockNode(node);
	
	pthread_mutex_lock(&filemutex);
    	fprintf(f,"Cliente %d %d %.3f %.3f %d\n", myid, numprod, in, b, codevisitate);
	pthread_mutex_unlock(&filemutex);
    	
	if(porte==1)
		AggiungiUscito(node, a);
	else 
	{
		LockArray(a);
		UnlockArrayAndSignal(a);		
		free(node);
	}
    	return NULL;
    
}


void * Cassa(void *arg)
{
	struct cassa *q  =  ((threadArgs_t*)arg)->q;
	int   myid  =  ((threadArgs_t*)arg)->thid;
	accesso *p=  ((threadArgs_t*)arg)->acc;
	int index = ((threadArgs_t*)arg)->index;
	FILE *f = ((threadArgs_t*)arg)->f;
	float apertura;
	int i;
	long prodotti_elaborati=0;
	long clienti_serviti=0;
	struct timespec support;
    
	converti_tempo(((threadArgs_t*)arg)->tempo_extra,&((q+index)->tempo_extra_t));
	converti_tempo(((threadArgs_t*)arg)->tempo_per_prodotto,&((q+index)->tempo_per_prodotto_t) );
    
	(q+index)->myid=myid;
	(q+index)->testa=NULL;
	(q+index)->coda=NULL;
	(q+index)->chiusure=0;
	(q+index)->lunghezza=0;
    
	if (pthread_mutex_init(&((q+index)->qlock), NULL) != 0)
	{
		perror("mutex init");
		return NULL;
	}
	if (pthread_cond_init(&((q+index)->qcond), NULL) != 0) 
	{
		perror("mutex cond");
		if (&((q+index)->qlock)) 
			pthread_mutex_destroy(&((q+index)->qlock));
		return NULL;
	}    
	struct client_node* data;
    
	while(supermercato==1)
	{     
		if((q+index)->chiuditi==0&&(q+index)->testa!=NULL&&(q+index)->aperta==1)
	        {
			data = pop((q+index),p);
			
	            	if (data == NULL)
	                	break;
			LockNode(data);
	            	assert(data);
	            	nanosleep(&((q+index)->tempo_extra_t), NULL);
            
			support.tv_nsec=((((q+index)->tempo_per_prodotto_t.tv_nsec/1000000)*data->nprod)%1000)*1000000; //faccio cosi' per non superare i limiti del long
	        	support.tv_sec=((q+index)->tempo_per_prodotto_t.tv_sec*data->nprod)+((((q+index)->tempo_per_prodotto_t.tv_nsec/1000000)*data->nprod)/1000);
	            	nanosleep(&(support), NULL);
            
	            	prodotti_elaborati=prodotti_elaborati+data->nprod;
			data->finito=1;
	            	data->in_servizio=0;
	            	UnlockNode(data);
			clienti_serviti++;
	        }
	        else if((q+index)->chiuditi==1)
	        {
	        	LockQueue(q+index);
			(q+index)->chiusure=(q+index)->chiusure+1;
			(q+index)->testa=NULL;
    			(q+index)->coda=NULL;
    			(q+index)->lunghezza=0;
			clock_gettime(CLOCK_MONOTONIC, &((q+index)->chiudi_t));
			(q+index)->tempo_apertura_t=((q+index)->chiudi_t.tv_sec-(q+index)->apri_t.tv_sec)+((float)(((q+index)->chiudi_t.tv_nsec-(q+index)->apri_t.tv_nsec)/1000000)/1000)+(q+index)->tempo_apertura_t;
            		(q+index)->aperta=0;
            		(q+index)->chiuditi=0;
            		UnlockQueue(q+index);
		}
        	else if(porte==0&&(q+index)->lunghezza==0)
        		break;
    	}
    
   	if((q+index)->aperta==1)
	{
		LockQueue(q+index);
		(q+index)->aperta=0;
		(q+index)->chiusure=(q+index)->chiusure+1;
		clock_gettime(CLOCK_MONOTONIC, &((q+index)->chiudi_t));
		(q+index)->tempo_apertura_t=((q+index)->chiudi_t.tv_sec-(q+index)->apri_t.tv_sec)+((float)(((q+index)->chiudi_t.tv_nsec-(q+index)->apri_t.tv_nsec)/1000000)/1000)+(q+index)->tempo_apertura_t;
		UnlockQueue(q+index);
	}

	while(clienti==1)
		sleep(1);
	
	if((q+index)->tempo_apertura_t==0&&(q+index)->chiusure==0)
	{
		pthread_mutex_lock(&filemutex);
		fprintf(f,"Cassa %d %ld %ld 0 0 0\n", myid, prodotti_elaborati, clienti_serviti);
		pthread_mutex_unlock(&filemutex);
	}		
	else
	{    
		pthread_mutex_lock(&filemutex);		
		fprintf(f,"Cassa %d %ld %ld %.3f %.3f %d\n", myid, prodotti_elaborati, clienti_serviti, (q+index)->tempo_apertura_t, (q+index)->tempo_apertura_t/(q+index)->chiusure, (q+index)->chiusure);
		pthread_mutex_unlock(&filemutex);
	}
	return NULL;
}




void* DirettoreCOM(void* arg)
{
	struct cassa *q  = ((threadArgs_t*)arg)->q;
	int   myid  = ((threadArgs_t*)arg)->thid;
	accesso *p=  ((threadArgs_t*)arg)->acc;
	no_prod* a = ((threadArgs_t*)arg)->no_p;
	FILE *fpoi = ((threadArgs_t*)arg)->f;
	int i;
	int num_casse_troppi_clienti=0;
	int num_casse_pochi_clienti=0;
	int num=0;
	struct timespec attesa;
	converti_tempo(distanza_tra_controlli,&attesa);
    
	
	struct sockaddr_un serv_addr;
	int sockfd;
	int cassa;
	int newstate;
	int code=3;
    
	sockfd=socket(AF_UNIX, SOCK_STREAM, 0);
	memset(&serv_addr, '0', sizeof(serv_addr));
	serv_addr.sun_family = AF_UNIX;    
	strncpy(serv_addr.sun_path,SOCKNAME, strlen(SOCKNAME)+1);
	while (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1 )
	{	
		if ( errno == ENOENT )
			sleep(1); // sock non esiste 
		else exit(EXIT_FAILURE); 
	}
   
	writen(sockfd, &code, sizeof(int));
	printf("comunicatore con direttore connesso\n");
    
	writen(sockfd, &K, sizeof(int));
	writen(sockfd, &S1, sizeof(int));
	writen(sockfd, &S2, sizeof(int));
	
	pthread_t    *th;
	threadArgs_t *thARGS;
    
	th     = malloc(sizeof(pthread_t));
	thARGS = malloc(sizeof(threadArgs_t));
		
	thARGS->thid = 0;
	thARGS->q    = q;
	thARGS->acc=p;
	thARGS->no_p=a;
	thARGS->index=0;
    
	if (pthread_create(th, NULL, Uscita_No_Prodotti, thARGS) != 0)
	{
		fprintf(stderr, "pthread_create failed (Producer)\n");
        	exit(EXIT_FAILURE);
	}

	pthread_t    *thSig;
	threadArgs_t *thARGSSig;
    
	thSig     = malloc(sizeof(pthread_t));
	thARGSSig = malloc(sizeof(threadArgs_t));

	thARGSSig->f=fpoi;
    
	if (pthread_create(thSig, NULL, Attesa, thARGSSig) != 0)
	{
        	fprintf(stderr, "pthread_create failed (Producer)\n");
        	exit(EXIT_FAILURE);
	}
	int cassa_in_chiusura=1;       
	while(supermercato==1)
	{
		
		nanosleep(&attesa,NULL);
        
        	for(i=0;i<K;i++)
        	{
            		if((q+i)->chiuditi==1)
	    			writen(sockfd, &cassa_in_chiusura, sizeof(int));
	    		else writen(sockfd, &((q+i)->aperta), sizeof(int));
            		writen(sockfd, &((q+i)->lunghezza), sizeof(int));	
        	}
        
        
        	while(supermercato==1)
        	{
			readn(sockfd,&cassa,sizeof(int));
			if(cassa!=-1)
			{
				readn(sockfd,&newstate,sizeof(int));
				if(newstate==1)
				{				
					printf("chiudo la %d\n", cassa);					
					(q+cassa)->chiuditi=1;
				}
				else if(newstate==0)
				{					
					printf("apro la %d\n", cassa);					
					(q+cassa)->aperta=1;
                			clock_gettime(CLOCK_MONOTONIC, &((q+i)->apri_t));
				}
			}
			else break;
        	}
        
    	}
	i=-1;
    	writen(sockfd, &i, sizeof(int));
    	readn(sockfd,&cassa,sizeof(int));
	while(cassa!=-2)
		readn(sockfd,&cassa,sizeof(int));	

    	pthread_join(*thSig, NULL);
	free(thSig);
	free(thARGSSig);
	
	pthread_join(*th, NULL);
	free(th);
	free(thARGS);

    	close(sockfd);
    return NULL;
}




int main(int argc, char *argv[])
{    
	FILE *f;
	FILE *fd;
	f = fopen("report.log", "w");
	fd = fopen(argv[1], "r");
	char* buffer; 
	buffer=malloc(MAX_LEN*sizeof(char));
	printf("supermercato avviato\n");
	int i;
	int j=0;
	int t;
	int num_casse_aperte;
	int index;
        
	fgets(buffer,MAX_LEN,fd);
	K=atoi(buffer);
	if(K<0)
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);	
	C=atoi(buffer);
	if(C<1)
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);
	E=atoi(buffer);
	if(!(0<E&&E<C))
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);
	t=atoi(buffer);
	if(t<10)
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);
	P=atoi(buffer);
	if(P<0)
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);	
	S=atoi(buffer);
	if(S<0)
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);
	S1=atoi(buffer);
	if(!(0<S1<K))
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);
	S2=atoi(buffer);
	if(S2<0)
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);
	distanza_tra_controlli=atoi(buffer);	//e' in millisecondi
	if(distanza_tra_controlli<0)
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	fgets(buffer,MAX_LEN,fd);
	num_casse_aperte=atoi(buffer);
	if(!(0<num_casse_aperte&&num_casse_aperte<K))
	{
		fprintf(stderr,"argomento non valido\n");
		return -1;
	}
	
	converti_tempo(t,&T);
    
  	j=C;
  	
  	no_prod np;
  	int id_usciti[E];
	accesso usciti;
	usciti.e=id_usciti;
	usciti.num_usciti=0;

	if(pthread_mutex_init(&filemutex, NULL)!= 0)
	{
		perror("mutex init");
       		return -1;
	}
	
	if (pthread_mutex_init(&usciti.lock, NULL) != 0)
	{
		perror("mutex init");
        	return -1;
	}
    
	if (pthread_cond_init(&usciti.cond, NULL) != 0) 
	{
		perror("mutex cond");
		if (&usciti.cond) 
			pthread_cond_destroy(&usciti.cond);
		return -1;
	}  
	
	if (pthread_mutex_init(&np.lock, NULL) != 0)
	{
        	perror("mutex init");
        	return -1;
	} 
	if (pthread_mutex_init(&np.richieste, NULL) != 0)
	{
        	perror("mutex init");
        	return -1;
	}  
	if (pthread_cond_init(&np.cond, NULL) != 0) 
	{
		perror("mutex cond");
		if (&np.cond) 
			pthread_cond_destroy(&np.cond);	//perchè?
		return -1;
	}    
    
	pthread_t    *th;
	threadArgs_t *thARGS;
    
	th     = malloc((K+C+1)*sizeof(pthread_t));
	thARGS = malloc((K+C+1)*sizeof(threadArgs_t));
	if (!th || !thARGS) 
	{
		fprintf(stderr, "malloc fallita\n");
        	exit(EXIT_FAILURE);
	}
    
	struct cassa q[K];
    
	for(i=0;i<num_casse_aperte;i++)
	{
		q[i].aperta=1;
		q[i].chiuditi=0;
		clock_gettime(CLOCK_MONOTONIC, &q[i].apri_t);
		q[i].tempo_apertura_t=0;
	}	
    
	for(i=num_casse_aperte;i<K;i++)
	{
		q[i].aperta=0;
		q[i].chiuditi=0;
		q[i].tempo_apertura_t=0;
	}
    	 
	for(i=0;i<K; i++) 
	{    //argomenti per le casse
        	thARGS[i].thid = i;
        	thARGS[i].q    = q;
        	thARGS[i].acc=&usciti;
        	thARGS[i].index=i;
        	fgets(buffer,MAX_LEN,fd);
		thARGS[i].tempo_extra=atoi(buffer);
		if(!(19<thARGS[i].tempo_extra&&thARGS[i].tempo_extra<81))
		{
			fprintf(stderr,"argomento non valido\n");
			return -1;
		}
		fgets(buffer,MAX_LEN,fd);
        	thARGS[i].tempo_per_prodotto=atoi(buffer);
		if(thARGS[i].tempo_per_prodotto<0)
		{
			fprintf(stderr,"argomento non valido\n");
			return -1;
		}
        	thARGS[i].f=f;
	}
    	for(i=K;i<(K+C); i++) 
	{    //argomenti per i clienti
        	thARGS[i].thid = i-K;
       		thARGS[i].q    = q;
        	thARGS[i].acc=&usciti;
        	thARGS[i].no_p=&np;
        	thARGS[i].index= i-K;
        	thARGS[i].f=f;
	}
	i=K+C;
	//argomenti per il comunicatore con direttore
	thARGS[i].thid = 0;
	thARGS[i].q    = q;
	thARGS[i].acc=&usciti;
	thARGS[i].no_p=&np;
	thARGS[i].index=0;
    
    
	for(i=0;i<K; i++)
        	if (pthread_create(&th[i], NULL, Cassa, &thARGS[i]) != 0)
        	{
            		fprintf(stderr, "pthread_create failed (Producer)\n");
            		exit(EXIT_FAILURE);
        	}
	for(i=0;i<C; i++)
        	if (pthread_create(&th[K+i], NULL, Cliente, &thARGS[K+i]) != 0)
        	{
            		fprintf(stderr, "pthread_create failed (Consumer)\n");
            		exit(EXIT_FAILURE);
        	}
    
    
	if (pthread_create(&th[K+C], NULL, DirettoreCOM, &thARGS[K+C]) != 0)
	{
        	fprintf(stderr, "pthread_create failed (Consumer)\n");
        	exit(EXIT_FAILURE);
	}
	while(porte==1)
	{
        	LockArray(&usciti);

        	while(usciti.num_usciti<E&&porte==1)
		{			
			UnlockArrayAndWait(&usciti);
		}
		
		if(porte==0)
		{	
			UnlockArray(&usciti);			
			break;
		}
	
        	for(i=0;i<E;i++)
        	{	
            		index=id_usciti[i];
            		thARGS[K+index].thid=j;
            		pthread_create(&th[K+index], NULL, Cliente, &thARGS[K+index]);//qui istanzio un cliente
            		j++;   
        	}
        	usciti.num_usciti=0;
        	UnlockArray(&usciti);
    	}
   	for(i=K;i<K+C;i++)	
		pthread_join(th[i], NULL);
	supermercato=0;
	clienti=0;

	for(i=0;i<K;i++)	
		pthread_join(th[i], NULL);
	no_products=0;
	
	SendASignal(&np);

	pthread_join(th[K+C], NULL);
	free(th);
	free(thARGS);
	free(buffer);
	fclose(f);
	fclose(fd);
	printf("supermercato termina\n");//per testare se arriva fino in fondo
	return 0;
}

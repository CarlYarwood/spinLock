#include <time.h>
#include <pthread.h>
#include "rdma_common.h"

#define noop (void)0

pthread_mutex_t *out_lock = NULL;

struct rdma_client_in {
	uint64_t node_id;
	int critical_section;
	int noncritical_section;
	int num_aquire;
};

struct c_mcs_ctx {
	struct rdma_cm_id* client_id;
    struct rdma_event_channel* cm_event_channel;
	struct ibv_pd* pd;
    struct ibv_comp_channel* comp;
	struct ibv_cq* cq;
	struct ibv_mr* response_mr;
    struct ibv_mr* metadata_mr;
	struct ibv_mr* server_metadata_mr;
    struct ibv_mr* client_metadata_mr;
	struct rdma_buffer_attr* server_metadata_attr;
    struct rdma_buffer_attr* client_metadata_attr;
};

struct c_mcs_ctx* build_client_spin_context(struct rdma_cm_id* client_id, struct rdma_event_channel* cm_event_channel, uint64_t *response, uint64_t *metadata) {
	struct c_mcs_ctx *ctx = NULL;
	struct ibv_pd* pd = NULL;
    struct ibv_comp_channel* comp = NULL;
    struct ibv_cq* cq = NULL;
    struct ibv_mr *response_mr = NULL;
    struct ibv_mr *metadata_mr = NULL;
    struct ibv_mr *server_metadata_mr = NULL;
    struct ibv_mr *client_metadata_mr = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct rdma_buffer_attr *server_metadata_attr = NULL;
    struct rdma_buffer_attr *client_metadata_attr = NULL;
	struct ibv_sge server_recv_sge;
	struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;

	ctx = (struct c_mcs_ctx*)malloc(sizeof(struct c_mcs_ctx));
    server_metadata_attr = (struct rdma_buffer_attr *)malloc(sizeof(struct rdma_buffer_attr));
    client_metadata_attr = (struct rdma_buffer_attr *)malloc(sizeof(struct rdma_buffer_attr));

	pd = ibv_alloc_pd(client_id->verbs);
	if (!pd) {
		rdma_error("Failed to alloc pd, errno: %d \n", -errno);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
	}
    comp = ibv_create_comp_channel(client_id->verbs);
	if (!comp) {
		rdma_error("Failed to create IO completion event channel, errno: %d\n", -errno);
		ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
	    return NULL;
	}

    cq = ibv_create_cq(client_id->verbs, CQ_CAPACITY, NULL, comp, 0);
	if (!cq) {
		rdma_error("Failed to create CQ, errno: %d \n", -errno);
		ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
	}

	if (ibv_req_notify_cq(cq, 0)) {
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		ibv_destroy_cq(cq);
		ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
	}
    bzero(&qp_init_attr, sizeof qp_init_attr);
    qp_init_attr.cap.max_recv_sge = MAX_SGE;
    qp_init_attr.cap.max_recv_wr = MAX_WR;
    qp_init_attr.cap.max_send_sge = MAX_SGE;
    qp_init_attr.cap.max_send_wr = MAX_WR;
    qp_init_attr.qp_type = IBV_QPT_RC;

    qp_init_attr.recv_cq = cq;
    qp_init_attr.send_cq = cq;

	if (rdma_create_qp(client_id, pd, &qp_init_attr)) {
		rdma_error("Failed to create QP, errno: %d \n", -errno);
		ibv_destroy_cq(cq);
		ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
	    return NULL;
	}

    response_mr = rdma_buffer_register(pd, response, sizeof(*response), (IBV_ACCESS_LOCAL_WRITE));
	if(!response_mr){
		perror("Failed to setup response mr\n");
		rdma_destroy_qp(client_id);
		ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
	}

    metadata_mr = rdma_buffer_register(pd, metadata, sizeof(uint64_t) * 2, (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));
    if(!metadata_mr) {
        perror("Failed to setup metadata mr\n");
        rdma_destroy_qp(client_id);
        rdma_buffer_free(response_mr);
		ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
    }

    (*client_metadata_attr).address = (uint64_t) metadata_mr->addr;
    (*client_metadata_attr).length = (uint32_t) metadata_mr->length;
    (*client_metadata_attr).stag.remote_stag = (uint32_t) metadata_mr->rkey;

    client_metadata_mr = rdma_buffer_register(pd, client_metadata_attr, sizeof(*client_metadata_attr), (IBV_ACCESS_LOCAL_WRITE));
    if(!client_metadata_mr) {
        rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		rdma_destroy_qp(client_id);
		rdma_buffer_free(response_mr);
        rdma_buffer_free(metadata_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
    }

	server_metadata_mr = rdma_buffer_register(pd, server_metadata_attr, sizeof(*server_metadata_attr), (IBV_ACCESS_LOCAL_WRITE));
	if(!server_metadata_mr){
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		rdma_destroy_qp(client_id);
		rdma_buffer_free(response_mr);
        rdma_buffer_free(metadata_mr);
        rdma_buffer_free(client_metadata_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
	}

	server_recv_sge.addr = (uint64_t) server_metadata_mr->addr;
	server_recv_sge.length = (uint32_t) server_metadata_mr->length;
	server_recv_sge.lkey = (uint32_t) server_metadata_mr->lkey;

	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	if (ibv_post_recv(client_id->qp , &server_recv_wr, &bad_server_recv_wr)) {
		perror("Failed to pre-post the receive buffer, errno: %d \n");
		rdma_destroy_qp(client_id);
		rdma_buffer_free(server_metadata_mr);
		rdma_buffer_free(response_mr);
        rdma_buffer_free(metadata_mr);
        rdma_buffer_free(client_metadata_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
        free(client_metadata_attr);
		return NULL;
	}
	debug("Receive buffer pre-posting is successful \n");

	ctx->client_id = client_id;
    ctx->cm_event_channel = cm_event_channel;
	ctx->pd = pd;
	ctx->comp = comp;
	ctx->cq = cq;
	ctx->response_mr = response_mr;
    ctx->metadata_mr = metadata_mr;
	ctx->server_metadata_mr = server_metadata_mr;
    ctx->client_metadata_mr = client_metadata_mr;
	ctx->server_metadata_attr = server_metadata_attr;
    ctx->client_metadata_attr = client_metadata_attr;

	return ctx;
}

int send_client_metadata(struct c_mcs_ctx *ctx) {
    struct ibv_wc wc;
    struct ibv_sge client_send_sge;
    struct ibv_send_wr client_send_wr, *bad_client_send_wr = NULL;


    client_send_sge.addr = (uint64_t)(ctx->client_metadata_attr);
    client_send_sge.length = sizeof(*(ctx->client_metadata_attr));
    client_send_sge.lkey = (ctx->client_metadata_mr)->lkey;

    bzero(&client_send_wr, sizeof(client_send_wr));
    client_send_wr.sg_list = &client_send_sge;
    client_send_wr.num_sge = 1;
    client_send_wr.opcode = IBV_WR_SEND;
    client_send_wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send((ctx->client_id)->qp, &client_send_wr, &bad_client_send_wr)) {
	    rdma_error("Posting of server metdata failed, errno: %d \n", -errno);
	    return -errno;
    }

    if (process_work_completion_events((ctx->comp), &wc, 2) != 2) {
	    perror("Failed to send server metadata, ret = %d \n");
	    return -1;
    }
    printf("metadata sent\n");
    return 0;
}

int destroy_context(struct c_mcs_ctx* ctx){
	int ret = 0;
	rdma_destroy_qp(ctx->client_id);

	if (rdma_destroy_id(ctx->client_id)) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		ret = -1;
	}

	if (ibv_destroy_cq(ctx->cq)) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		ret = -1;
	}

	if (ibv_destroy_comp_channel(ctx->comp)) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		ret = -1;
		// we continue anyways;
	}

	/* Destroy memory buffers */
	rdma_buffer_deregister(ctx->server_metadata_mr);
	rdma_buffer_deregister(ctx->response_mr);
    rdma_buffer_deregister(ctx->metadata_mr);
    rdma_buffer_deregister(ctx->client_metadata_mr);

	if (ibv_dealloc_pd(ctx->pd)) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		ret = -1;
		// we continue anyways;
	}

	free(ctx->server_metadata_attr);
    free(ctx->client_metadata_attr);

    rdma_destroy_event_channel(ctx->cm_event_channel);

	return ret;
}



int copmare_and_swap(struct c_mcs_ctx* ctx, uint64_t cmp, uint64_t swap) {
    uint64_t ret = -1;
    struct ibv_send_wr cas_wr, *bad_cas_wr = NULL;
    struct ibv_wc cas_wc;
    struct ibv_sge cas_sge;

    cas_sge.addr = (uint64_t) (ctx->response_mr)->addr;
    cas_sge.length = (uint64_t) (ctx->response_mr)->length;
    cas_sge.lkey = (uint64_t)(ctx->response_mr)->lkey;
    
    bzero(&cas_wr, sizeof(cas_wr));
    cas_wr.sg_list = &cas_sge;
    cas_wr.num_sge = 1;
    cas_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    cas_wr.wr.atomic.rkey = (ctx->server_metadata_attr)->stag.remote_stag;
    cas_wr.wr.atomic.remote_addr = (ctx->server_metadata_attr)->address;
    cas_wr.wr.atomic.compare_add = cmp;
    cas_wr.wr.atomic.swap = swap;
    cas_wr.send_flags = IBV_SEND_SIGNALED;

    ret = ibv_post_send((ctx->client_id)->qp, &cas_wr, &bad_cas_wr);
    if(ret) {
        perror("Failed to send cas\n");
        return 1;
    }

    if (process_work_completion_events(ctx->comp, &cas_wc, 1) != 1) {
        perror("We failed to get 1 work completions\n");
        return 1;
    }
    return 0;
}

int acquire_lock(struct c_mcs_ctx * ctx,uint64_t *node_id, uint64_t *response) {
	do {
        copmare_and_swap(ctx, 0, *node_id);
    } while(*response != 0);
	return 0;
}

int release_lock(struct c_mcs_ctx *ctx, uint64_t* node_id, uint64_t *response) {
	copmare_and_swap(ctx, *node_id, 0);
    if(*response != *node_id) {
        perror("lock release failed\n");
        return -1;
    }
    // printf("lock release successful\n");
	return 0;
}

struct c_mcs_ctx* mcs_connect(struct sockaddr_in* server_sockaddr, uint64_t *response, uint64_t *metadata) {
	struct c_mcs_ctx *ctx = NULL;
	struct rdma_cm_id *cm_client_id = NULL;
	struct rdma_cm_event *cm_event = NULL;
    struct rdma_event_channel *cm_event_channel = NULL;
	struct rdma_conn_param conn_param;
	struct ibv_wc wc;

    cm_event_channel = rdma_create_event_channel();
    if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
		return NULL;
	}

	if (rdma_create_id(cm_event_channel, &cm_client_id, NULL, RDMA_PS_TCP)) {
		rdma_error("Creating cm id failed with errno: %d \n", -errno); 
		return NULL;
	}

	if (rdma_resolve_addr(cm_client_id, NULL, (struct sockaddr*) server_sockaddr, 2000)) {
		rdma_error("Failed to resolve address, errno: %d \n", -errno);
		return NULL;
	}

	if (process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ADDR_RESOLVED, &cm_event)) {
		perror("Failed to receive a valid event, ret = %d \n");
		return NULL;
	}

	if (rdma_ack_cm_event(cm_event)) {
		rdma_error("Failed to acknowledge the CM event, errno: %d\n", -errno);
		return NULL;
	}

	if (rdma_resolve_route(cm_client_id, 2000)) {
		rdma_error("Failed to resolve route, erno: %d \n", -errno);
	       return NULL;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");

	ctx = build_client_spin_context(cm_client_id, cm_event_channel, response, metadata);
	if (!ctx) {
		perror("Failed to build context\n");
		return NULL;
	}

	if (process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ROUTE_RESOLVED, &cm_event)) {
		perror("Failed to receive a valid event, ret = %d \n");
		return NULL;
	}

    if (rdma_ack_cm_event(cm_event)) {
		rdma_error("Failed to acknowledge the CM event, errno: %d \n", -errno);
		return NULL;
	}

	

    bzero(&conn_param, sizeof(conn_param));
	conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 3;
	if (rdma_connect(ctx->client_id, &conn_param)) {
		rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
		return NULL;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");

	if (process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ESTABLISHED, &cm_event)) {
		perror("Failed to get cm event, ret = %d \n");
	    return NULL;
	}

	if (rdma_ack_cm_event(cm_event)) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", -errno);
		return NULL;
	}

    if(send_client_metadata(ctx)) {
        perror("Failed to send client metadata\n");
        return NULL;
    }

	return ctx;
}

int mcs_disconnect(struct c_mcs_ctx* ctx){
	struct rdma_cm_event *cm_event = NULL;
	int ret = 0;
	if (rdma_disconnect(ctx->client_id)) {
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		ret = -1;
		//continuing anyways
	}
	if (process_rdma_cm_event(ctx->cm_event_channel, RDMA_CM_EVENT_DISCONNECTED, &cm_event)) {
		perror("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n");
		ret = -1;
		//continuing anyways 
	}
	if (rdma_ack_cm_event(cm_event)) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", -errno);
		ret = -1;
		//continuing anyways
	}
			
	if(destroy_context(ctx)) {
		perror("Failed to detroy context fully");
		ret = -1;
	}

	free(ctx);

	return ret;
}

void * rdma_client(void * in) {
	struct c_mcs_ctx *ctx = NULL;
	struct sockaddr_in server_sockaddr;
	uint64_t *response = calloc(1, sizeof(uint64_t));
    uint64_t *node_id = calloc(1, sizeof(uint64_t));
	uint64_t *metadata = calloc(2, sizeof(uint64_t));
	int critical_section = ((struct rdma_client_in *) in)->critical_section;
	int noncritical_section = ((struct rdma_client_in *) in)->noncritical_section;
	int num_aquire = ((struct rdma_client_in *) in)->num_aquire;
	// clock_t b_acquire, e_acquire, b_release, e_release;
	clock_t start, end;
	*node_id = ((struct rdma_client_in *) in)->node_id;
    metadata[NEXT] = 0;
    metadata[NOTIFY] = 0;

    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET;    
    if (get_addr(address[0], (struct sockaddr*) &server_sockaddr)) {
		rdma_error("Invalid IP \n");
		return NULL;
	}
    server_sockaddr.sin_port = htons(port[0]);
	
	ctx = mcs_connect(&server_sockaddr, response, metadata);
	start = clock();

	for (int i = 0; i < num_aquire; i++) {
		for (int i = 0; i < noncritical_section; i++) {
			noop;
		}
		//lock
		// b_acquire = clock();
		acquire_lock(ctx, node_id, response);
		// e_acquire = clock();
		// printf("%f l\n", ((double)(e_acquire-b_acquire)/CLOCKS_PER_SEC));
		//work
		for (int i=0; i < critical_section; i++) {
			noop;
		}
		//unlock
		// b_release = clock();
		release_lock(ctx, node_id, response);
		// e_release = clock();

		// printf("%f u\n", ((double)(e_release-b_release)/CLOCKS_PER_SEC));
	}
	end = clock();

	mcs_disconnect(ctx);
	/* We free the buffers */
	free(node_id);
	free(response);
    free(metadata);

	pthread_mutex_lock(out_lock);
	printf("%f\n",((double)(num_aquire * critical_section))/((double)(end-start)/CLOCKS_PER_SEC));
	pthread_mutex_unlock(out_lock);
	return NULL;
}

int main(int argc, char** argv) {
	struct rdma_client_in *in = NULL;
    int option, noncritical_section, critical_section, num_aquire, num_threads;
	uint64_t id;
	pthread_t *clients = NULL;
	out_lock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(out_lock, NULL);
	noncritical_section = 1;
	critical_section = 1;
	num_aquire = 1;
	num_threads = 1;
	id = 1;

    bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    while ((option = getopt(argc, argv, "a:p:c:n:l:i:t:")) != -1) {
		switch (option) {
			case 'c':
				critical_section = atoi(optarg);
				break;
			case 'n':
				noncritical_section = atoi(optarg);
				break;
			case 'l':
				num_aquire = atoi(optarg);
				break;
			case 'i':
				id = strtoul(optarg, NULL, 0);
				break;
			case 't':
				num_threads = atoi(optarg);
				break;
			default:
				return -1;
				break;
		}
	}

	clients = (pthread_t *)malloc(sizeof(pthread_t) * num_threads);
	in = (struct rdma_client_in *)malloc(sizeof(struct rdma_client_in) * num_threads);

	for (int i = 0; i < num_threads; i++) {
		(&in[i])->node_id = id;
		(&in[i])->critical_section = critical_section;
		(&in[i])->noncritical_section = noncritical_section;
		(&in[i])->num_aquire = num_aquire;
		pthread_create(&clients[i], NULL, rdma_client, (void *) &in[i]);
		id++;
	}

	for(int i = 0; i < num_threads; i++) {
		pthread_join(clients[i], NULL);
	}
	pthread_mutex_destroy(out_lock);
	free(in);
	free(clients);
	free(out_lock);
	return 0;
}
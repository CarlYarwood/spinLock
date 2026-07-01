#include "rdma_common.h"

#define noop (void)0

struct c_ticket_ctx {
	struct rdma_cm_id* client_id;
	struct ibv_pd* pd;
    struct ibv_comp_channel* comp;
	struct ibv_cq* cq;
	struct ibv_mr* response_mr;
	struct ibv_mr* server_metadata_mr;
	struct rdma_buffer_attr* server_metadata_attr;
};

uint64_t *response = NULL;

struct c_ticket_ctx* build_client_ticket_context(struct rdma_cm_id* client_id) {
	struct c_ticket_ctx *ctx = NULL;
	struct ibv_pd* pd = NULL;
    struct ibv_comp_channel* comp = NULL;
    struct ibv_cq* cq = NULL;
    struct ibv_mr *response_mr = NULL;
    struct ibv_mr *server_metadata_mr = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct rdma_buffer_attr *server_metadata_attr = NULL;
    struct rdma_conn_param conn_param;
	struct ibv_sge server_recv_sge;
	struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;

	ctx = (struct c_ticket_ctx*)malloc(sizeof(struct c_ticket_ctx));
    server_metadata_attr = (struct rdma_buffer_attr *)malloc(sizeof(struct rdma_buffer_attr));

	pd = ibv_alloc_pd(client_id->verbs);
	if (!pd) {
		rdma_error("Failed to alloc pd, errno: %d \n", -errno);
		free(ctx);
		free(server_metadata_attr);
		return NULL;
	}
    comp = ibv_create_comp_channel(client_id->verbs);
	if (!comp) {
		rdma_error("Failed to create IO completion event channel, errno: %d\n", -errno);
		ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
	    return NULL;
	}

    cq = ibv_create_cq(client_id->verbs, CQ_CAPACITY, NULL, comp, 0);
	if (!cq) {
		rdma_error("Failed to create CQ, errno: %d \n", -errno);
		ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
		return NULL;
	}

	if (ibv_req_notify_cq(cq, 0)) {
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		ibv_destroy_cq(cq);
		ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
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
		return NULL;
	}

	server_metadata_mr = rdma_buffer_register(pd, server_metadata_attr, sizeof(*server_metadata_attr), (IBV_ACCESS_LOCAL_WRITE));
	if(!server_metadata_mr){
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		rdma_destroy_qp(client_id);
		rdma_buffer_free(response_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
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
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
		free(ctx);
		free(server_metadata_attr);
		return NULL;
	}
	debug("Receive buffer pre-posting is successful \n");

	ctx->client_id = client_id;
	ctx->pd = pd;
	ctx->comp = comp;
	ctx->cq = cq;
	ctx->response_mr = response_mr;
	ctx->server_metadata_mr = server_metadata_mr;
	ctx->server_metadata_attr = server_metadata_attr;

	return ctx;
}

int destroy_context(struct c_ticket_ctx* ctx){
	rdma_destroy_qp(ctx->client_id);

	if (rdma_destroy_id(ctx->client_id)) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
	}

	if (ibv_destroy_cq(ctx->cq)) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
	}

	if (ibv_destroy_comp_channel(ctx->comp)) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}

	/* Destroy memory buffers */
	rdma_buffer_deregister(ctx->server_metadata_mr);
	rdma_buffer_deregister(ctx->response_mr);

	if (ibv_dealloc_pd(ctx->pd)) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}

	free(ctx->server_metadata_attr);

	return 0;
}

int fetch_and_add(struct c_ticket_ctx* ctx, int offset) {
    int ret = -1;
    struct ibv_send_wr cas_wr, *bad_cas_wr = NULL;
    struct ibv_wc cas_wc;
    struct ibv_sge cas_sge;

    cas_sge.addr = (uint64_t) (ctx->response_mr)->addr;
    cas_sge.length = (uint64_t) (ctx->response_mr)->length;
    cas_sge.lkey = (uint64_t) (ctx->response_mr)->lkey;
    
    bzero(&cas_wr, sizeof(cas_wr));
    cas_wr.sg_list = &cas_sge;
    cas_wr.num_sge = 1;
    cas_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    cas_wr.wr.atomic.rkey = (ctx->server_metadata_attr)->stag.remote_stag;
    cas_wr.wr.atomic.remote_addr = (ctx->server_metadata_attr)->address + sizeof(uint64_t) * offset;
    cas_wr.wr.atomic.compare_add = 1;
    cas_wr.send_flags = IBV_SEND_SIGNALED;

    ret = ibv_post_send((ctx->client_id)->qp, &cas_wr, &bad_cas_wr);
    if(ret) {
        perror("Failed to send cas\n");
        return 1;
    }
    ret = process_work_completion_events(ctx->comp, &cas_wc, 1);
    if (ret != 1) {
        perror("We failed to get 1 work completions\n");
        return 1;
    }
    return 0;
}

int rdma_read(struct c_ticket_ctx *ctx, int offset) {
	int ret = -1;
    struct ibv_send_wr read_wr, *bad_read_wr = NULL;
    struct ibv_wc read_wc;
    struct ibv_sge read_sge;

    read_sge.addr = (uint64_t) (ctx->response_mr)->addr;
    read_sge.length = (uint64_t) (ctx->response_mr)->length;
    read_sge.lkey = (uint64_t)(ctx->response_mr)->lkey;

	bzero(&read_wr, sizeof(read_wr));
    read_wr.sg_list = &read_sge;
    read_wr.num_sge = 1;
    read_wr.opcode = IBV_WR_RDMA_READ;
	read_wr.send_flags = IBV_SEND_SIGNALED;

	read_wr.wr.rdma.rkey = (ctx->server_metadata_attr)->stag.remote_stag;
    read_wr.wr.rdma.remote_addr = (ctx->server_metadata_attr)->address + sizeof(uint64_t) * offset;

	ret = ibv_post_send((ctx->client_id)->qp, &read_wr, &bad_read_wr);
    if(ret) {
        perror("Failed to send read\n");
        return 1;
    }
    ret = process_work_completion_events(ctx->comp, &read_wc, 1);
    if (ret != 1) {
        perror("We failed to get 1 work completions\n");
        return 1;
    }
    return 0;

}

struct c_ticket_ctx* connect_to_server(struct rdma_event_channel* cm_event_channel, struct sockaddr_in* server_sockaddr) {
	struct c_ticket_ctx *ctx = NULL;
	struct rdma_cm_id *cm_client_id = NULL;
	struct rdma_cm_event *cm_event = NULL;
	struct rdma_conn_param conn_param;
	struct ibv_wc wc;

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

	ctx = build_client_ticket_context(cm_client_id);
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

	printf("The client is connected successfully \n");
	if(process_work_completion_events(ctx->comp, &wc, 1) != 1) {
		perror("We failed to get 1 work completions \n");
		return NULL;
	}

	return ctx;
}

int disconnect_from_server(struct rdma_event_channel* cm_event_channel, struct c_ticket_ctx* ctx){
	struct rdma_cm_event *cm_event = NULL;
	int ret = 0;
	if (rdma_disconnect(ctx->client_id)) {
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		ret = -1;
		//continuing anyways
	}
	if (process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_DISCONNECTED, &cm_event)) {
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

uint64_t acquire_lock(struct c_ticket_ctx*  ctx) {
	int test = 1;
	uint64_t ticket;
	do {
		test = fetch_and_add(ctx, NEXT);
	} while(test != 0);
	ticket = *response;
	do {
		if(rdma_read(ctx, NOW) != 0) {
			printf("read fail\n");
		}
	} while(ticket != *response);

	return ticket;
}

int release_lock(struct c_ticket_ctx* ctx, uint64_t ticket) {
	fetch_and_add(ctx, NOW);
    if(*response != ticket) {
        perror("de-latch failed\n");
        return -1;
    }
    printf("de-latch successful\n");
	return 0;
}

int main(int argc, char** argv) {
	struct sockaddr_in server_sockaddr;
    struct rdma_event_channel *cm_event_channel = NULL;
    struct c_ticket_ctx *ctx = NULL;
    int option, critical_section, noncritical_section;
	uint64_t ticket;
	clock_t b_setup, e_setup, b_acquire, e_acquire, b_release, e_release, b_shutdown, e_shutdown; 
	b_setup = clock();
    response = calloc(1, sizeof(uint64_t));
    *response = 1;
	critical_section = 1;
	noncritical_section = 1;

    bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    while ((option = getopt(argc, argv, "a:p:c:n:")) != -1) {
		switch (option) {
			case 'a':
				if (get_addr(optarg, (struct sockaddr*) &server_sockaddr)) {
					rdma_error("Invalid IP \n");
					return -1;
				}
				break;
			case 'p':
				server_sockaddr.sin_port = htons(strtol(optarg, NULL, 0)); 
				break;
			case 'c':
				critical_section = atoi(optarg);
				break;
			case 'n':
				noncritical_section = atoi(optarg);
				break;
			default:
				return -1;
				break;
		}
	}
	if (!server_sockaddr.sin_port) {
	  /* no port provided, use the default port */
	  server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
	}

    cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
		return -errno;
	}

    ctx = connect_to_server(cm_event_channel, &server_sockaddr);
	e_setup = clock();
	printf("%f seconds to steup\n", ((double)(b_setup-e_setup)/CLOCKS_PER_SEC));

	for(int i = 0; i < noncritical_section; i++) {
		noop;
	}

    //lock
	b_acquire = clock();
    ticket = acquire_lock(ctx);
	e_acquire = clock();
    printf("lock acquired\n");
	printf("%f seconds to aquire\n", ((double)(b_acquire-e_acquire)/CLOCKS_PER_SEC));
    //work
	for (int i = 0; i < critical_section; i++) {
		noop;
	}
    //unlock
	b_release = clock();
	release_lock(ctx, ticket);
	e_release = clock();

	printf("%f seconds to release\n", ((double)(b_release-e_release)/CLOCKS_PER_SEC));


	b_shutdown = clock();
	disconnect_from_server(cm_event_channel, ctx);	
	/* We free the buffers */
	free(response);

	/* Destroy protection domain */
	
	rdma_destroy_event_channel(cm_event_channel);
	e_shutdown = clock();
	printf("Client resource clean up is complete \n");
	printf("%f seconds to shutdown\n", ((double)(b_shutdown-e_shutdown)/CLOCKS_PER_SEC));
	return 0;
}
    
#include <time.h>
#include <pthread.h>
#include "rdma_common.h"

char* address[NUM_NODES + 1] = {
    "10.10.1.1",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
	"10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
	"10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.2",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
	"10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
	"10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3",
    "10.10.1.3"
};

long port[NUM_NODES + 1] = {
    DEFAULT_RDMA_PORT,
    DEFAULT_RDMA_PORT,
    DEFAULT_RDMA_PORT + 1,
    DEFAULT_RDMA_PORT + 2,
    DEFAULT_RDMA_PORT + 3,
    DEFAULT_RDMA_PORT + 4,
    DEFAULT_RDMA_PORT + 5,
    DEFAULT_RDMA_PORT + 6,
	DEFAULT_RDMA_PORT + 7,
	DEFAULT_RDMA_PORT + 8,
    DEFAULT_RDMA_PORT + 9,
    DEFAULT_RDMA_PORT + 10,
    DEFAULT_RDMA_PORT + 11,
    DEFAULT_RDMA_PORT + 12,
    DEFAULT_RDMA_PORT + 13,
	DEFAULT_RDMA_PORT + 14,
	DEFAULT_RDMA_PORT + 15,
    DEFAULT_RDMA_PORT + 16,
    DEFAULT_RDMA_PORT + 17,
    DEFAULT_RDMA_PORT + 18,
    DEFAULT_RDMA_PORT + 19,
	DEFAULT_RDMA_PORT,
    DEFAULT_RDMA_PORT + 1,
    DEFAULT_RDMA_PORT + 2,
    DEFAULT_RDMA_PORT + 3,
    DEFAULT_RDMA_PORT + 4,
    DEFAULT_RDMA_PORT + 5,
    DEFAULT_RDMA_PORT + 6,
	DEFAULT_RDMA_PORT + 7,
	DEFAULT_RDMA_PORT + 8,
    DEFAULT_RDMA_PORT + 9,
    DEFAULT_RDMA_PORT + 10,
    DEFAULT_RDMA_PORT + 11,
    DEFAULT_RDMA_PORT + 12,
    DEFAULT_RDMA_PORT + 13,
	DEFAULT_RDMA_PORT + 14,
	DEFAULT_RDMA_PORT + 15,
    DEFAULT_RDMA_PORT + 16,
    DEFAULT_RDMA_PORT + 17,
    DEFAULT_RDMA_PORT + 18,
    DEFAULT_RDMA_PORT + 19,
};

struct rdma_client_in {
	uint64_t node_id;
	int critical_section;
	int noncritical_section;
	int num_aquire;
};

struct c_mcs_ctx {
    uint64_t* node_id;
    struct ibv_pd* pd;
    struct ibv_comp_channel* comp;
    struct ibv_cq* cq;
    struct ibv_mr* buffer_mr;
    struct ibv_mr* metadata_mr;
    struct ibv_mr* server_metadata_mr;
    struct ibv_mr* client_metadata_mr;
    struct rdma_buffer_attr* server_metadata_attr;
    struct rdma_buffer_attr* client_metadata_attr;
};

void noop(volatile int *dummy) {
    *dummy = *dummy; 
}

struct c_mcs_ctx* build_mcs_context(struct rdma_cm_id* client_id, volatile uint64_t *metadata, uint64_t *buffer, uint64_t* node_id) {
    struct c_mcs_ctx* ctx;
    struct ibv_pd* pd = NULL;
    struct ibv_comp_channel* comp = NULL;
    struct ibv_cq* cq = NULL;
    struct ibv_mr *buffer_mr = NULL;
    struct ibv_mr *metadata_mr = NULL;
    struct ibv_mr *server_metadata_mr = NULL;
    struct ibv_mr *client_metadata_mr = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct rdma_buffer_attr *server_metadata_attr;
    struct rdma_buffer_attr *client_metadata_attr;
    struct rdma_conn_param conn_param;
    struct ibv_sge server_recv_sge;
    struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;
    
    ctx = (struct c_mcs_ctx*)malloc(sizeof(struct c_mcs_ctx));
    server_metadata_attr = (struct rdma_buffer_attr *)malloc(sizeof(struct rdma_buffer_attr));
    client_metadata_attr = (struct rdma_buffer_attr *) malloc(sizeof(struct rdma_buffer_attr));

    pd = ibv_alloc_pd(client_id->verbs);
    if (!pd) {
        rdma_error("Failed to allocate a protection domain errno: %d\n", -errno);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    comp = ibv_create_comp_channel(client_id->verbs);
    if(!comp) {
        rdma_error("Failed to create an I/O completion event channel, %d\n", -errno);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    cq = ibv_create_cq(client_id->verbs, CQ_CAPACITY, NULL, comp, 0);
    if (!cq) {
        rdma_error("Failed to create a completion queue (cq), errno: %d\n", -errno);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    if (ibv_req_notify_cq(cq,0)) {
        rdma_error("Failed to request notifications on CQ errno: %d \n", -errno);
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
        rdma_error("Failed to create QP due to errno: %d\n", -errno);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }
    
    buffer_mr = rdma_buffer_register(pd, buffer, sizeof(*buffer), (IBV_ACCESS_LOCAL_WRITE));
    if(!buffer_mr){
        rdma_error("Server failed to buffer memory region \n");
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    metadata_mr = rdma_buffer_register(pd, (void *) metadata, sizeof(uint64_t) * 3, (IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_ATOMIC));
    if(!metadata_mr){
        rdma_error("Server failed to create buffer memory region \n");
        rdma_buffer_deregister(buffer_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    (*client_metadata_attr).address = (uint64_t)metadata_mr->addr;
    (*client_metadata_attr).length = (uint32_t)metadata_mr->length;
    (*client_metadata_attr).stag.remote_stag = (uint32_t)metadata_mr->rkey;
    client_metadata_mr = rdma_buffer_register(pd, client_metadata_attr, sizeof(*client_metadata_attr), (IBV_ACCESS_LOCAL_WRITE));
    if(!client_metadata_mr){
        rdma_error("Server failed to create to hold server metadata \n");
        rdma_buffer_deregister(metadata_mr);
        rdma_buffer_deregister(buffer_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    server_metadata_mr = rdma_buffer_register(pd, server_metadata_attr, sizeof(struct rdma_buffer_attr), (IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE));
    if(!server_metadata_mr) {
        rdma_error("Server failed to create to hold server metadata \n");
        rdma_buffer_deregister(server_metadata_mr);
        rdma_buffer_deregister(metadata_mr);
        rdma_buffer_deregister(buffer_mr);
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

    bzero(&server_recv_wr, sizeof(struct ibv_recv_wr));
    server_recv_wr.sg_list = &server_recv_sge;
    server_recv_wr.num_sge = 1;
    if(ibv_post_recv(client_id->qp, &server_recv_wr, &bad_server_recv_wr)) {
        rdma_error("Server failed to create to hold server metadata \n");
        rdma_buffer_deregister(client_metadata_mr);
        rdma_buffer_deregister(server_metadata_mr);
        rdma_buffer_deregister(metadata_mr);
        rdma_buffer_deregister(buffer_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    (*ctx).node_id = node_id;
    (*ctx).pd = pd;
    (*ctx).comp = comp;
    (*ctx).cq = cq;
    (*ctx).buffer_mr = buffer_mr;
    (*ctx).metadata_mr = metadata_mr;
    (*ctx).server_metadata_mr = server_metadata_mr;
    (*ctx).client_metadata_mr = client_metadata_mr;
    (*ctx).server_metadata_attr = server_metadata_attr;
    (*ctx).client_metadata_attr = client_metadata_attr;
    return ctx;
}

int send_client_metadata(struct rdma_cm_id* client_id) {
    struct c_mcs_ctx * ctx = (struct c_mcs_ctx *) client_id->context;
    struct ibv_wc wc;
    struct ibv_sge server_send_sge;
    struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;

    server_send_sge.addr = (uint64_t)(ctx->client_metadata_attr);
    server_send_sge.length = sizeof(*(ctx->client_metadata_attr));
    server_send_sge.lkey = (ctx->client_metadata_mr)->lkey;

    bzero(&server_send_wr, sizeof(server_send_wr));
    server_send_wr.sg_list = &server_send_sge;
    server_send_wr.num_sge = 1;
    server_send_wr.opcode = IBV_WR_SEND;
    server_send_wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(client_id->qp, &server_send_wr, &bad_server_send_wr)) {
	    rdma_error("Posting of server metdata failed, errno: %d \n", -errno);
	    return -errno;
    }

    if (process_work_completion_events(ctx->cq, &wc, 2) != 2) {
	    perror("Failed to send server metadata, ret = %d \n");
	    return -1;
    }
    return 0;
}

int compare_and_swap(struct rdma_cm_id* client_id, uint64_t cmp, uint64_t swap, int offset) {
	struct c_mcs_ctx* ctx = (struct c_mcs_ctx *)client_id->context;
    uint64_t ret = -1;
    struct ibv_send_wr cas_wr, *bad_cas_wr = NULL;
    struct ibv_wc cas_wc;
    struct ibv_sge cas_sge;

    cas_sge.addr = (uint64_t) (ctx->buffer_mr)->addr;
    cas_sge.length = (uint64_t) (ctx->buffer_mr)->length;
    cas_sge.lkey = (uint64_t)(ctx->buffer_mr)->lkey;
    
    bzero(&cas_wr, sizeof(cas_wr));
    cas_wr.sg_list = &cas_sge;
    cas_wr.num_sge = 1;
    cas_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    cas_wr.wr.atomic.rkey = (ctx->server_metadata_attr)->stag.remote_stag;
    cas_wr.wr.atomic.remote_addr = (ctx->server_metadata_attr)->address + (sizeof(uint64_t) * offset);
    cas_wr.wr.atomic.compare_add = cmp;
    cas_wr.wr.atomic.swap = swap;
    cas_wr.send_flags = IBV_SEND_SIGNALED;

    ret = ibv_post_send(client_id->qp, &cas_wr, &bad_cas_wr);
    if(ret) {
        perror("Failed to send cas\n");
        return 1;
    }

    if (process_work_completion_events(ctx->cq, &cas_wc, 1) != 1) {
        perror("We failed to get 1 work completions\n");
        return 1;
    }
    return 0;
}

int fetch_and_add(struct rdma_cm_id* client_id, int offset) {
	struct c_mcs_ctx* ctx = (struct c_mcs_ctx *)client_id->context;
    int ret = -1;
    struct ibv_send_wr cas_wr, *bad_cas_wr = NULL;
    struct ibv_wc cas_wc;
    struct ibv_sge cas_sge;

    cas_sge.addr = (uint64_t) (ctx->buffer_mr)->addr;
    cas_sge.length = (uint64_t) (ctx->buffer_mr)->length;
    cas_sge.lkey = (uint64_t) (ctx->buffer_mr)->lkey;
    
    bzero(&cas_wr, sizeof(cas_wr));
    cas_wr.sg_list = &cas_sge;
    cas_wr.num_sge = 1;
    cas_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    cas_wr.wr.atomic.rkey = (ctx->server_metadata_attr)->stag.remote_stag;
    cas_wr.wr.atomic.remote_addr = (ctx->server_metadata_attr)->address + (sizeof(uint64_t) * offset);
    cas_wr.wr.atomic.compare_add = 1;
    cas_wr.send_flags = IBV_SEND_SIGNALED;

    ret = ibv_post_send(client_id->qp, &cas_wr, &bad_cas_wr);
    if(ret) {
        perror("Failed to send cas\n");
        return 1;
    }
    ret = process_work_completion_events(ctx->cq, &cas_wc, 1);
    if (ret != 1) {
        perror("We failed to get 1 work completions\n");
        return 1;
    }
    return 0;
}

int rdma_read(struct rdma_cm_id *client_id, int offset) {
	struct c_mcs_ctx *ctx = (struct c_mcs_ctx *)client_id->context;
	int ret = -1;
    struct ibv_send_wr read_wr, *bad_read_wr = NULL;
    struct ibv_wc read_wc;
    struct ibv_sge read_sge;

    read_sge.addr = (uint64_t) (ctx->buffer_mr)->addr;
    read_sge.length = (uint64_t) (ctx->buffer_mr)->length;
    read_sge.lkey = (uint64_t)(ctx->buffer_mr)->lkey;

	bzero(&read_wr, sizeof(read_wr));
    read_wr.sg_list = &read_sge;
    read_wr.num_sge = 1;
    read_wr.opcode = IBV_WR_RDMA_READ;
	read_wr.send_flags = IBV_SEND_SIGNALED;

	read_wr.wr.rdma.rkey = (ctx->server_metadata_attr)->stag.remote_stag;
    read_wr.wr.rdma.remote_addr = (ctx->server_metadata_attr)->address + sizeof(uint64_t) * offset;

	ret = ibv_post_send(client_id->qp, &read_wr, &bad_read_wr);
    if(ret) {
        perror("Failed to send read\n");
        return 1;
    }
    ret = process_work_completion_events(ctx->cq, &read_wc, 1);
    if (ret != 1) {
        perror("We failed to get 1 work completions\n");
        return 1;
    }
    return 0;

}

int acquire_lock(struct rdma_cm_id ** id_arr, uint64_t *node_id, uint64_t *buffer, volatile uint64_t* metadata) {
    metadata[NEXT] = 0;
    metadata[NOTIFY] = 0;
    uint64_t expected = 0;
    do {
        compare_and_swap(id_arr[SERVER], expected, *node_id, LOCK);
        if (expected == *buffer) {
            break;
        }
        expected = *buffer;
    } while(1);
    if (*buffer == 0) {
        return 0;
    }

    compare_and_swap(id_arr[*buffer], 0, *node_id, NEXT);
    do {} while (metadata[NOTIFY] == 0);
    return 0;
}

int release_lock(struct rdma_cm_id** id_arr, uint64_t* node_id, uint64_t *buffer, volatile uint64_t* metadata) {
	if (metadata[NEXT] == 0) {
        compare_and_swap(id_arr[SERVER], *node_id, 0, LOCK);
        if(*buffer == *node_id) {
            return 0;
        }
    }
    do {} while(metadata[NEXT] == 0);
    compare_and_swap(id_arr[metadata[NEXT]],0, 1, NOTIFY);
    return 0;
}

struct rdma_cm_id* mcs_connect(struct sockaddr_in* server_sockaddr, struct rdma_event_channel* cm_event_channel, uint64_t *node_id, uint64_t client_node_id, uint64_t *buffer, volatile uint64_t *metadata) {
	struct c_mcs_ctx *ctx = NULL;
	struct rdma_cm_id *cm_client_id = NULL;
	struct rdma_cm_event *cm_event = NULL;
	struct rdma_conn_param conn_param;
	struct ibv_wc wc;
	uint64_t* c_node_id = (uint64_t *)malloc(sizeof(uint64_t));
	*c_node_id = client_node_id;
	
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

	ctx = build_mcs_context(cm_client_id, metadata, buffer, c_node_id);
	if (!ctx) {
		perror("Failed to build context\n");
		return NULL;
	}
	cm_client_id->context = (void *)ctx;

	if (rdma_ack_cm_event(cm_event)) {
		rdma_error("Failed to acknowledge the CM event, errno: %d\n", -errno);
		return NULL;
	}

	if (rdma_resolve_route(cm_client_id, 2000)) {
		rdma_error("Failed to resolve route, erno: %d \n", -errno);
	       return NULL;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");

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
    conn_param.private_data = (void *) node_id;
    conn_param.private_data_len = sizeof(uint64_t);
	if (rdma_connect(cm_client_id, &conn_param)) {
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

    if(send_client_metadata(cm_client_id)) {
        perror("Failed to send client metadata\n");
        return NULL;
    }

	return cm_client_id;
}

int clean_up_context(struct rdma_cm_id* client_id) {
    struct c_mcs_ctx *ctx = (struct c_mcs_ctx *)client_id->context;
    rdma_destroy_qp(client_id);

    if (rdma_destroy_id(client_id)) {
	    printf("Failed to destroy client id cleanly, %d \n", -errno);
        return -errno;
	}

    if (ibv_destroy_cq(ctx->cq)) {
        printf("Failed to destroy completion queue cleanly, %d \n", -errno);
        return -errno;
    }

    if (ibv_destroy_comp_channel(ctx->comp)) {
        printf("Failed to destroy completion channel cleanly, %d \n", -errno);
        return -errno;
    }

    rdma_buffer_deregister(ctx->buffer_mr);
    rdma_buffer_deregister(ctx->metadata_mr);
    rdma_buffer_deregister(ctx->server_metadata_mr);
    rdma_buffer_deregister(ctx->client_metadata_mr);


    if (ibv_dealloc_pd(ctx->pd)) {
        printf("Failed to destroy client protection domain cleanly, %d \n", -errno);
        return -errno;
    }
    free(ctx->node_id);
    free(ctx->server_metadata_attr);
    free(ctx->client_metadata_attr);
    free(ctx);
    return 0;
}

int mcs_disconnect(struct rdma_cm_id* client_id, struct rdma_event_channel* cm_event_channel){
	struct rdma_cm_event *cm_event = NULL;
	int ret = 0;
	if (rdma_disconnect(client_id)) {
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
			
	if(clean_up_context(client_id)) {
		perror("Failed to detroy context fully");
		ret = -1;
	}

	return ret;
}

void wait_on_data(volatile uint64_t *data, uint64_t val) {
	do {} while(*data != val);
}

void* rdma_client(void *in) {
    volatile uint64_t *metadata = NULL;
	uint64_t *buffer = NULL;
	uint64_t *node_id = calloc(1, sizeof(uint64_t));
	int critical_section = ((struct rdma_client_in *) in)->critical_section;
	int noncritical_section = ((struct rdma_client_in *) in)->noncritical_section;
	int num_aquire = ((struct rdma_client_in *) in)->num_aquire;
    int num_conn = 0;
	struct sockaddr_in client_server_sockaddr, server_sockaddr;
    struct rdma_event_channel *cm_event_channel = NULL;
    struct rdma_cm_id *cm_server_id = NULL;
    struct rdma_cm_id ** id_arr;
	clock_t start, end;

	*node_id = ((struct rdma_client_in *) in)->node_id;
    id_arr = (struct rdma_cm_id **) malloc(sizeof(struct rdma_cm_id *) * (NUM_NODES + 1));

    for (int i = 0;  i < (NUM_NODES + 1); i++) {
        id_arr[i] = NULL;
    }

    metadata = calloc(3, sizeof(uint64_t));
    buffer = calloc(1, sizeof(uint64_t));
    metadata[NEXT] = 0;
    metadata[NOTIFY] = 0;
	metadata[SYNC] = 0;
	bzero(&client_server_sockaddr, sizeof client_server_sockaddr);
	client_server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	client_server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	client_server_sockaddr.sin_port = htons(port[*node_id]);

    cm_event_channel = rdma_create_event_channel();
    if (!cm_event_channel) {
        rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
		return NULL;
    }

	if (rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP)) {
		rdma_error("Creating server cm id failed with errno: %d ", -errno);
		return NULL;
	}

    if (rdma_bind_addr(cm_server_id, (struct sockaddr*) &client_server_sockaddr)) {
		rdma_error("Failed to bind server address, errno: %d \n", -errno);
		return NULL;
	}

	if (rdma_listen(cm_server_id, 8)) {
		rdma_error("rdma_listen failed to listen on server address, errno: %d ", -errno);
		return NULL;
	}
	
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET;    
    if (get_addr(address[0], (struct sockaddr*) &server_sockaddr)) {
		rdma_error("Invalid IP \n");
		return NULL;
	}
    server_sockaddr.sin_port = htons(port[0]);
	id_arr[SERVER] = mcs_connect(&server_sockaddr, cm_event_channel, node_id, SERVER, buffer, metadata);

    wait_on_data(&metadata[SYNC], 1);

    while(num_conn < (*node_id) - 1) {
        struct rdma_cm_event *cm_event = NULL;
        struct rdma_cm_id* client_id = NULL;
    
        if (rdma_get_cm_event(cm_event_channel, &cm_event)) {
		  rdma_error("Failed to retrieve a cm event, errno: %d \n", -errno);
		  return NULL;
        }

        if(0 != cm_event->status){
		    rdma_error("CM event has non zero status: %d\n", cm_event->status);
		    rdma_ack_cm_event(cm_event);
		    return NULL;
	    }

        switch (cm_event->event){
            case RDMA_CM_EVENT_CONNECT_REQUEST :
                struct c_mcs_ctx* ctx = NULL;
                struct rdma_conn_param conn_param;
                uint64_t* client_node_id;

                client_node_id = (uint64_t *) malloc(sizeof(uint64_t));
                
                client_id = cm_event->id;
                *client_node_id = *((uint64_t *) cm_event->param.conn.private_data);

                ctx = build_mcs_context(client_id, metadata, buffer, client_node_id);
                if(!ctx) {
                    rdma_ack_cm_event(cm_event);
                    perror("Failed to build client Context\n");
                    return NULL;
                }

                (client_id)->context = (void *)ctx;

                id_arr[*client_node_id] = client_id;

                if (rdma_ack_cm_event(cm_event)) {
                    rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
                    return NULL;
                }

                memset(&conn_param, 0, sizeof(conn_param));
                conn_param.initiator_depth = 3;
                conn_param.responder_resources = 3;
                if (rdma_accept(client_id, &conn_param)) {
	                rdma_error("Failed to accept the connection, errno: %d \n", -errno);
	                return NULL;
                }

                break;

            case RDMA_CM_EVENT_ESTABLISHED :
                client_id = cm_event->id;

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return NULL;
	            }

                if(send_client_metadata(client_id)) {
                     perror("Failed to send server metadata \n");
                     return NULL;
                }
				num_conn++;
                break;

            default:
                rdma_error("Unexpected event received: %s", rdma_event_str(cm_event->event));
		        rdma_ack_cm_event(cm_event);
		        return NULL;
        }
    }

	for (int i = (*node_id) + 1; i < NUM_NODES + 1; i++) {
        struct sockaddr_in client_sockaddr;
        bzero(&client_sockaddr, sizeof client_sockaddr);
        client_sockaddr.sin_family = AF_INET;
        if(get_addr(address[i], (struct sockaddr*) &client_sockaddr)) {
            rdma_error("Invalid IP \n");
            return NULL;
        }
        client_sockaddr.sin_port = htons(port[i]);

        id_arr[i] = mcs_connect(&client_sockaddr, cm_event_channel, node_id, i, buffer, metadata);
    }

    fetch_and_add(id_arr[SERVER], READY);

	wait_on_data(&metadata[SYNC], 2);

	

	start = clock();

	for (int i = 0; i < num_aquire; i++) {
		// pre work
		for (int i = 0; i < noncritical_section; i++) {
			noop(&i);
		}
		// lock
		acquire_lock(id_arr, node_id, buffer, metadata);
		// work
		for (int i=0; i < critical_section; i++) {
			noop(&i);
		}
		// unlock
		release_lock(id_arr, node_id, buffer, metadata);;
	}

	end = clock();

	printf("%f\n",((double)(num_aquire * critical_section))/((double)(end-start)/CLOCKS_PER_SEC));
	return NULL;

	do {
		struct rdma_cm_event *cm_event = NULL;
        struct rdma_cm_id* client_id = NULL;
    
        if (rdma_get_cm_event(cm_event_channel, &cm_event)) {
		  rdma_error("Failed to retrieve a cm event, errno: %d \n", -errno);
		  return NULL;
        }

        if(0 != cm_event->status){
		    rdma_error("CM event has non zero status: %d\n", cm_event->status);
		    rdma_ack_cm_event(cm_event);
		    return NULL;
	    }
		switch(cm_event->event) {
			case RDMA_CM_EVENT_DISCONNECTED :
                client_id = cm_event->id;

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return NULL;
	            }
                id_arr[(*((struct c_mcs_ctx *)(client_id->context))->node_id)] = NULL;

                if (clean_up_context(client_id)) {
                    perror("failed to cleanup client context");
                    return NULL;
                }

                num_conn--;
                break;
			default:
                rdma_error("Unexpected event received: %s", rdma_event_str(cm_event->event));
		        rdma_ack_cm_event(cm_event);
		        return NULL;
		}
	} while (num_conn > (*node_id));

	for (int i = (*node_id) - 1; i<=0 + 1; i--) {
        mcs_disconnect(id_arr[i], cm_event_channel);
		id_arr[i] = NULL;
    }

	free(node_id);
    free(buffer);
    free((void *)metadata);
    free(id_arr);

	if (rdma_destroy_id(cm_server_id)) {
		rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
	}
	rdma_destroy_event_channel(cm_event_channel);
	return NULL;
}

int main(int argc, char** argv) {
	struct rdma_client_in *client_in = NULL;
    int option, noncritical_section, critical_section, num_aquire, num_threads;
	uint64_t id;
	pthread_t *clients = NULL;
	noncritical_section = 1;
	critical_section = 1;
	num_aquire = 1;
	num_threads = 1;
	id = 1;

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
	client_in = (struct rdma_client_in *)malloc(sizeof(struct rdma_client_in) * num_threads);

	for (int i = 0; i < num_threads; i++) {
		(&client_in[i])->node_id = id + i;
		(&client_in[i])->critical_section = critical_section;
		(&client_in[i])->noncritical_section = noncritical_section;
		(&client_in[i])->num_aquire = num_aquire;
        
		pthread_create(&clients[i], NULL, rdma_client, (void *) &client_in[i]);
	}

	for(int i = 0; i < num_threads; i++) {
		pthread_join(clients[i], NULL);
	}
	free(client_in);
	free(clients);
	return 0;
}

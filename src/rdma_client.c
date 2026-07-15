#include <time.h>
#include <pthread.h>
#include "rdma_common.h"

#define noop (void)0

#define TOTAL_NODES 2

pthread_mutex_t *out_lock = NULL;
char* address[TOTAL_NODES + 1] = {
    "128.110.219.58",
    "128.110.219.43",
    "128.110.219.43"
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84",
    // "128.110.219.84"
};
long port[TOTAL_NODES + 1] = {
    DEFAULT_RDMA_PORT,
    DEFAULT_RDMA_PORT,
    DEFAULT_RDMA_PORT + 1
    // DEFAULT_RDMA_PORT + 2,
    // DEFAULT_RDMA_PORT + 3,
    // DEFAULT_RDMA_PORT + 4,
    // DEFAULT_RDMA_PORT + 5,
    // DEFAULT_RDMA_PORT + 6,
    // DEFAULT_RDMA_PORT + 7,
    // DEFAULT_RDMA_PORT + 8,
    // DEFAULT_RDMA_PORT + 9,
    // DEFAULT_RDMA_PORT + 10,
    // DEFAULT_RDMA_PORT + 11,
    // DEFAULT_RDMA_PORT + 12,
    // DEFAULT_RDMA_PORT + 13,
    // DEFAULT_RDMA_PORT + 14
};

struct rdma_client_in {
	uint64_t node_id;
	int critical_section;
	int noncritical_section;
	int num_aquire;
    uint64_t *buffer;
    uint64_t *metadata;
};

struct c_s_mcs_ctx {
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

struct rdma_server_in {
    long port;
    struct rdma_client_in* in;
};

struct c_mcs_ctx {
	struct rdma_cm_id* client_id;
    struct rdma_event_channel* cm_event_channel;
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

struct c_s_mcs_ctx* build_server_mcs_context(struct rdma_cm_id* client_id, uint64_t *metadata, uint64_t *buffer) {
    struct c_s_mcs_ctx* ctx;
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
    
    ctx = (struct c_s_mcs_ctx*)malloc(sizeof(struct c_s_mcs_ctx));
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

    metadata_mr = rdma_buffer_register(pd, metadata, sizeof(uint64_t) * 2, (IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_ATOMIC));
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

    (*server_metadata_attr).address = (uint64_t)metadata_mr->addr;
    (*server_metadata_attr).length = (uint32_t)metadata_mr->length;
    (*server_metadata_attr).stag.remote_stag = (uint32_t)metadata_mr->rkey;
    server_metadata_mr = rdma_buffer_register(pd, server_metadata_attr, sizeof(*server_metadata_attr), (IBV_ACCESS_LOCAL_WRITE));
    if(!server_metadata_mr){
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



    client_metadata_mr = rdma_buffer_register(pd, client_metadata_attr, sizeof(struct rdma_buffer_attr), (IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE));
    if(!client_metadata_mr) {
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

    server_recv_sge.addr = (uint64_t)client_metadata_mr->addr;
    server_recv_sge.length = (uint32_t)client_metadata_mr->length;
    server_recv_sge.lkey = (uint32_t) client_metadata_mr->lkey;

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

int send_server_metadata(struct rdma_cm_id* client_id) {
    struct c_s_mcs_ctx * ctx = (struct c_s_mcs_ctx *) client_id->context;
    struct ibv_wc wc;
    struct ibv_sge server_send_sge;
    struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;


    server_send_sge.addr = (uint64_t)(ctx->server_metadata_attr);
    server_send_sge.length = sizeof(*(ctx->server_metadata_attr));
    server_send_sge.lkey = (ctx->server_metadata_mr)->lkey;

    bzero(&server_send_wr, sizeof(server_send_wr));
    server_send_wr.sg_list = &server_send_sge;
    server_send_wr.num_sge = 1;
    server_send_wr.opcode = IBV_WR_SEND;
    server_send_wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(client_id->qp, &server_send_wr, &bad_server_send_wr)) {
	    rdma_error("Posting of server metdata failed, errno: %d \n", -errno);
	    return -errno;
    }

    if (process_work_completion_events((ctx->comp), &wc, 2) != 2) {
	    perror("Failed to send server metadata, ret = %d \n");
	    return -1;
    }
    return 0;
}

struct c_mcs_ctx* build_client_spin_context(struct rdma_cm_id* client_id, struct rdma_event_channel* cm_event_channel, uint64_t *buffer, uint64_t *metadata) {
	struct c_mcs_ctx *ctx = NULL;
	struct ibv_pd* pd = NULL;
    struct ibv_comp_channel* comp = NULL;
    struct ibv_cq* cq = NULL;
    struct ibv_mr *buffer_mr = NULL;
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

    buffer_mr = rdma_buffer_register(pd, buffer, sizeof(*buffer), (IBV_ACCESS_LOCAL_WRITE));
	if(!buffer_mr){
		perror("Failed to setup buffer mr\n");
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
        rdma_buffer_free(buffer_mr);
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
		rdma_buffer_free(buffer_mr);
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
		rdma_buffer_free(buffer_mr);
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
		rdma_buffer_free(buffer_mr);
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
	ctx->buffer_mr = buffer_mr;
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
	rdma_buffer_deregister(ctx->buffer_mr);
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

int rdma_write(struct c_mcs_ctx *ctx, int offset) {
	int ret = -1;
    struct ibv_send_wr write_wr, *bad_write_wr = NULL;
    struct ibv_wc write_wc;
    struct ibv_sge write_sge;

    write_sge.addr = (uint64_t) (ctx->buffer_mr)->addr;
    write_sge.length = (uint64_t) (ctx->buffer_mr)->length;
    write_sge.lkey = (uint64_t)(ctx->buffer_mr)->lkey;

	bzero(&write_wr, sizeof(write_wr));
    write_wr.sg_list = &write_sge;
    write_wr.num_sge = 1;
    write_wr.opcode = IBV_WR_RDMA_WRITE;
	write_wr.send_flags = IBV_SEND_SIGNALED;

	write_wr.wr.rdma.rkey = (ctx->server_metadata_attr)->stag.remote_stag;
    write_wr.wr.rdma.remote_addr = (ctx->server_metadata_attr)->address + sizeof(uint64_t) * offset;

	ret = ibv_post_send((ctx->client_id)->qp, &write_wr, &bad_write_wr);
    if(ret) {
        perror("Failed to send read\n");
        return 1;
    }
    ret = process_work_completion_events(ctx->comp, &write_wc, 1);
    if (ret != 1) {
        perror("We failed to get 1 work completions\n");
        return 1;
    }
    return 0;

}



int copmare_and_swap(struct c_mcs_ctx* ctx, uint64_t cmp, uint64_t swap, int offset) {
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

int acquire_lock(struct c_mcs_ctx ** ctx_arr,uint64_t *node_id, uint64_t *buffer, uint64_t* metadata) {
    printf("node %lu aquire lock\n", *node_id);
    uint64_t expected = 0;
    do {
        copmare_and_swap(ctx_arr[SERVER], expected, *node_id, LOCK);
        if (expected == *buffer) {
            break;
        }
        expected = *buffer;
    } while(1);
    if (*buffer == 0) {
        printf("node %lu lock uncontested\n", *node_id);
        return 0;
    }
    copmare_and_swap(ctx_arr[*buffer], 0, *node_id, NEXT);
    printf("node %lu wait on notify\n", *node_id);
    while(metadata[NOTIFY] == 0) {
        // printf("node %lu waiting on Notify\n", *node_id);
    }
    printf("node %lu notify release\n", *node_id);
    return 0;
}

int release_lock(struct c_mcs_ctx** ctx_arr, uint64_t* node_id, uint64_t *buffer, uint64_t* metadata) {
    printf("node %lu release lock\n", *node_id);
	if (metadata[NEXT] == 0) {
        copmare_and_swap(ctx_arr[SERVER], *node_id, 0, LOCK);
        if(*buffer == *node_id) {
            return 0;
        }
        while(metadata[NEXT] == 0) {
            printf("node %lu waiting on Next\n", *node_id);
        }
    }
    printf("node %lu metadata Next %lu\n", *node_id, metadata[NEXT]);
    copmare_and_swap(ctx_arr[metadata[NEXT]], 0, 1, NOTIFY);
    printf("node %lu metadata cas response %lu\n", *node_id, *buffer);
    metadata[NEXT] = 0;
    metadata[NOTIFY] = 0;
}

struct c_mcs_ctx* mcs_connect(struct sockaddr_in* server_sockaddr, uint64_t *node_id, uint64_t *buffer, uint64_t *metadata) {
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

    printf("RDMA_CM_EVENT_ADDR_RESOLVED\n");
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

	ctx = build_client_spin_context(cm_client_id, cm_event_channel, buffer, metadata);
	if (!ctx) {
		perror("Failed to build context\n");
		return NULL;
	}

    printf("RDMA_CM_EVENT_ROUTE_RESOLVED\n");
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
	if (rdma_connect(ctx->client_id, &conn_param)) {
		rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
		return NULL;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");

    printf("RDMA_CM_EVENT_EVENT_ESTABLISHED\n");
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

    printf("RDMA_CM_EVENT_ADDR_DISCONNECTED\n");
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

int clean_up_context(struct rdma_cm_id* client_id) {
    struct c_s_mcs_ctx *ctx = (struct c_s_mcs_ctx *)client_id->context;
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
    free(ctx->server_metadata_attr);
    free(ctx->client_metadata_attr);
    free(ctx);
    return 0;
}

void * rdma_client(void * in) {
	struct c_mcs_ctx **ctx_arr = NULL;
	struct sockaddr_in server_sockaddr;
	uint64_t *buffer = ((struct rdma_client_in *) in)->buffer;
    uint64_t *node_id = calloc(1, sizeof(uint64_t));
	uint64_t *metadata = ((struct rdma_client_in *) in)->metadata;
	int critical_section = ((struct rdma_client_in *) in)->critical_section;
	int noncritical_section = ((struct rdma_client_in *) in)->noncritical_section;
	int num_aquire = ((struct rdma_client_in *) in)->num_aquire;
	// clock_t b_acquire, e_acquire, b_release, e_release;
	clock_t start, end;
	*node_id = ((struct rdma_client_in *) in)->node_id;
    printf("node id: %lu\n", *node_id);

    ctx_arr = (struct c_mcs_ctx **)malloc(sizeof(struct c_mcs_ctx*) * (TOTAL_NODES + 1));
    for (int i = 0; i< TOTAL_NODES + 1; i++){
        ctx_arr[i] = NULL;
    }

    sleep(10);
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET;    
    if (get_addr(address[0], (struct sockaddr*) &server_sockaddr)) {
		rdma_error("Invalid IP \n");
		return NULL;
	}
    server_sockaddr.sin_port = htons(port[0]);
	
	ctx_arr[SERVER] = mcs_connect(&server_sockaddr, node_id, buffer, metadata);

    for(int i = 1; i < TOTAL_NODES + 1; i++) {
        if(i != *node_id) {
            struct sockaddr_in client_sockaddr;
            bzero(&client_sockaddr, sizeof client_sockaddr);
            client_sockaddr.sin_family = AF_INET;
            if(get_addr(address[i], (struct sockaddr*) &client_sockaddr)) {
                rdma_error("Invalid IP \n");
                return NULL;
            }
            client_sockaddr.sin_port = htons(port[i]);
            ctx_arr[i] = mcs_connect(&client_sockaddr, node_id, buffer, metadata);
        }
    }
	start = clock();

	for (int i = 0; i < num_aquire; i++) {
		for (int i = 0; i < noncritical_section; i++) {
			noop;
		}
		//lock
		// b_acquire = clock();
		acquire_lock(ctx_arr, node_id, buffer, metadata);
		// e_acquire = clock();
		// printf("%f l\n", ((double)(e_acquire-b_acquire)/CLOCKS_PER_SEC));
		//work
		for (int i=0; i < critical_section; i++) {
			noop;
		}
		//unlock
		// b_release = clock();
		release_lock(ctx_arr, node_id, buffer, metadata);
		// e_release = clock();

		// printf("%f u\n", ((double)(e_release-b_release)/CLOCKS_PER_SEC));
	}
	end = clock();

    for (int i = 0; i<TOTAL_NODES + 1; i++) {
        if(i != *node_id) {
            mcs_disconnect(ctx_arr[i]);
        }
    }
	/* We free the buffers */
	free(node_id);

	pthread_mutex_lock(out_lock);
	printf("%f\n",((double)(num_aquire * critical_section))/((double)(end-start)/CLOCKS_PER_SEC));
	pthread_mutex_unlock(out_lock);
	return NULL;
}

void* rdma_server(void *in) {
    uint64_t *metadata = NULL, *buffer = NULL;
    int option, num_conn = 0;
	struct sockaddr_in server_sockaddr;
    struct rdma_event_channel *cm_event_channel = NULL;
    struct rdma_cm_id *cm_server_id = NULL;
    long port = ((struct rdma_server_in *)in)->port;
    struct rdma_client_in *client_in = ((struct rdma_server_in *)in)->in;
    pthread_t * client = NULL;

    client = (pthread_t *)malloc(sizeof(pthread_t));

    metadata = calloc(2, sizeof(uint64_t));
    buffer = calloc(1, sizeof(uint64_t));
    metadata[NEXT] = 0;
    metadata[NOTIFY] = 0;
    client_in->buffer = buffer;
    client_in->metadata = metadata;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	server_sockaddr.sin_port = htons(port);

    cm_event_channel = rdma_create_event_channel();
    if (!cm_event_channel) {
        rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
		return NULL;
    }

	if (rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP)) {
		rdma_error("Creating server cm id failed with errno: %d ", -errno);
		return NULL;
	}

    if (rdma_bind_addr(cm_server_id, (struct sockaddr*) &server_sockaddr)) {
		rdma_error("Failed to bind server address, errno: %d \n", -errno);
		return NULL;
	}

	if (rdma_listen(cm_server_id, 8)) {
		rdma_error("rdma_listen failed to listen on server address, errno: %d ", -errno);
		return NULL;
	}

    pthread_create(client, NULL, rdma_client, (void *)client_in);
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

        switch (cm_event->event){
            case RDMA_CM_EVENT_CONNECT_REQUEST :
                printf("server RDMA_CM_EVENT_CONNECT_REQUEST\n");
                struct c_s_mcs_ctx* ctx = NULL;
                struct rdma_conn_param conn_param;
                
                client_id = cm_event->id;
                printf("%lu\n", *((uint64_t *) cm_event->param.conn.private_data));

                ctx = build_server_mcs_context(client_id, metadata, buffer);
                if(!ctx) {
                    rdma_ack_cm_event(cm_event);
                    perror("Failed to build client Context\n");
                    return NULL;
                }

                (client_id)->context = (void *)ctx;

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

                num_conn++;
                break;

            case RDMA_CM_EVENT_ESTABLISHED :
                printf("server RDMA_CM_EVENT_ESTABLISHED\n");
                client_id = cm_event->id;

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return NULL;
	            }

                if(send_server_metadata(client_id)) {
                     perror("Failed to send server metadata \n");
                     return NULL;
                }
                break;

            case RDMA_CM_EVENT_DISCONNECTED :
                printf("serve RDMA_CM_EVENT_DISCONNECTED");
                client_id = cm_event->id;

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return NULL;
	            }

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
    } while(num_conn > 0);

    pthread_join(*client, NULL);

    free(buffer);
    free(metadata);

	if (rdma_destroy_id(cm_server_id)) {
		rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
	}
	rdma_destroy_event_channel(cm_event_channel);
	return NULL;
}

int main(int argc, char** argv) {
	struct rdma_client_in *client_in = NULL;
    struct rdma_server_in *server_in = NULL;
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
    server_in = (struct rdma_server_in *)malloc(sizeof(struct rdma_server_in) * num_threads);
	client_in = (struct rdma_client_in *)malloc(sizeof(struct rdma_client_in) * num_threads);

	for (int i = 0; i < num_threads; i++) {
		(&client_in[i])->node_id = id + i;
		(&client_in[i])->critical_section = critical_section;
		(&client_in[i])->noncritical_section = noncritical_section;
		(&client_in[i])->num_aquire = num_aquire;
        (&client_in[i])->buffer = NULL;
        (&client_in[i])->metadata = NULL;
        (&server_in[i])->port = DEFAULT_RDMA_PORT + i;
        (&server_in[i])->in = &client_in[i];
        
		pthread_create(&clients[i], NULL, rdma_server, (void *) &server_in[i]);
	}

	for(int i = 0; i < num_threads; i++) {
		pthread_join(clients[i], NULL);
	}
	pthread_mutex_destroy(out_lock);
	free(client_in);
    free(server_in);
	free(clients);
	free(out_lock);
	return 0;
}

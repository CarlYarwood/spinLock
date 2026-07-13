#include "rdma_common.h"

struct s_mcs_ctx {
    struct ibv_pd* pd;
    struct ibv_comp_channel* comp;
    struct ibv_cq* cq;
    struct ibv_mr* lock_mr;
    struct ibv_mr* server_metadata_mr;
    struct ibv_mr* client_metadata_mr;
    struct rdma_buffer_attr* server_metadata_attr;
    struct rdma_buffer_attr* client_metadata_attr;
};

struct s_mcs_ctx* build_server_mcs_context(struct rdma_cm_id* client_id, uint64_t *lock) {
    struct s_mcs_ctx* ctx;
    struct ibv_pd* pd = NULL;
    struct ibv_comp_channel* comp = NULL;
    struct ibv_cq* cq = NULL;
    struct ibv_mr *lock_mr = NULL;
    struct ibv_mr *server_metadata_mr = NULL;
    struct ibv_mr *client_metadata_mr = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct rdma_buffer_attr *server_metadata_attr;
    struct rdma_buffer_attr *client_metadata_attr;
    struct rdma_conn_param conn_param;
    struct ibv_sge server_recv_sge;
    struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;
    
    ctx = (struct s_mcs_ctx*)malloc(sizeof(struct s_mcs_ctx));
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

    lock_mr = rdma_buffer_register(pd, lock, sizeof(uint64_t) * 2, (IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_ATOMIC));
    if(!lock_mr){
        rdma_error("Server failed to create lock memory region \n");
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        free(client_metadata_attr);
        return NULL;
    }

    (*server_metadata_attr).address = (uint64_t)lock_mr->addr;
    (*server_metadata_attr).length = (uint32_t)lock_mr->length;
    (*server_metadata_attr).stag.remote_stag = (uint32_t)lock_mr->rkey;
    server_metadata_mr = rdma_buffer_register(pd, server_metadata_attr, sizeof(*server_metadata_attr), (IBV_ACCESS_LOCAL_WRITE));
    if(!server_metadata_mr){
        rdma_error("Server failed to create to hold server metadata \n");
        rdma_buffer_deregister(lock_mr);
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
        rdma_buffer_deregister(lock_mr);
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
        rdma_buffer_deregister(lock_mr);
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
    (*ctx).lock_mr = lock_mr;
    (*ctx).server_metadata_mr = server_metadata_mr;
    (*ctx).client_metadata_mr = client_metadata_mr;
    (*ctx).server_metadata_attr = server_metadata_attr;
    (*ctx).client_metadata_attr = client_metadata_attr;
    printf("context built\n");
    return ctx;
}

int send_server_metadata(struct rdma_cm_id* client_id) {
    struct s_mcs_ctx * ctx = (struct s_mcs_ctx *) client_id->context;
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
    printf("metadata sent\n");
    return 0;
}

int clean_up_context(struct rdma_cm_id* client_id) {
    struct s_mcs_ctx *ctx = (struct s_mcs_ctx *)client_id->context;
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
    rdma_buffer_deregister(ctx->lock_mr);
    rdma_buffer_deregister(ctx->server_metadata_mr);
    rdma_buffer_deregister(ctx->client_metadata_mr);


    if (ibv_dealloc_pd(ctx->pd)) {
        printf("Failed to destroy client protection domain cleanly, %d \n", -errno);
        return -errno;
    }
    free(ctx->server_metadata_attr);
    free(ctx->client_metadata_attr);
    free(ctx);
    printf("context cleaned up\n");
    return 0;
}

int main(int argc, char** argv) {
    uint64_t *lock = NULL;
    int option, num_conn = 0;
	struct sockaddr_in server_sockaddr;
    struct rdma_event_channel *cm_event_channel = NULL;
    struct rdma_cm_id *cm_server_id = NULL;

    lock = calloc(2, sizeof(uint64_t));
    lock[LOCK] = 0;
    lock[CLOCK] = 0;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);

    cm_event_channel = rdma_create_event_channel();
    if (!cm_event_channel) {
        rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
		return -errno;
    }

	if (rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP)) {
		rdma_error("Creating server cm id failed with errno: %d ", -errno);
		return -errno;
	}

    if (rdma_bind_addr(cm_server_id, (struct sockaddr*) &server_sockaddr)) {
		rdma_error("Failed to bind server address, errno: %d \n", -errno);
		return -errno;
	}

	if (rdma_listen(cm_server_id, 8)) {
		rdma_error("rdma_listen failed to listen on server address, errno: %d ", -errno);
		return -errno;
	}

    do {
        struct rdma_cm_event *cm_event = NULL;
        struct rdma_cm_id* client_id = NULL;
    
        if (rdma_get_cm_event(cm_event_channel, &cm_event)) {
		  rdma_error("Failed to retrieve a cm event, errno: %d \n", -errno);
		  return -errno;
        }

        if(0 != cm_event->status){
		    rdma_error("CM event has non zero status: %d\n", cm_event->status);
		    rdma_ack_cm_event(cm_event);
		    return -(cm_event->status);
	    }

        switch (cm_event->event){
            case RDMA_CM_EVENT_CONNECT_REQUEST :
                struct s_mcs_ctx* ctx = NULL;
                struct rdma_conn_param conn_param;
                
                client_id = cm_event->id;
                printf("%lu\n", (uint64_t) cm_event->param.conn.private_data);

                ctx = build_server_mcs_context(client_id, lock);
                if(!ctx) {
                    rdma_ack_cm_event(cm_event);
                    perror("Failed to build client Context\n");
                    return -1;
                }

                (client_id)->context = (void *)ctx;

                if (rdma_ack_cm_event(cm_event)) {
                    rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
                    return -errno;
                }

                memset(&conn_param, 0, sizeof(conn_param));
                conn_param.initiator_depth = 3;
                conn_param.responder_resources = 3;
                if (rdma_accept(client_id, &conn_param)) {
	                rdma_error("Failed to accept the connection, errno: %d \n", -errno);
	                return -errno;
                }

                num_conn++;
                break;

            case RDMA_CM_EVENT_ESTABLISHED :
                client_id = cm_event->id;

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return -errno;
	            }

                if(send_server_metadata(client_id)) {
                     perror("Failed to send server metadata \n");
                     return -1;
                }
                break;

            case RDMA_CM_EVENT_DISCONNECTED :
                client_id = cm_event->id;

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return -errno;
	            }

                if (clean_up_context(client_id)) {
                    perror("failed to cleanup client context");
                    return -1;
                }

                num_conn--;
                break;
            default:
                rdma_error("Unexpected event received: %s", rdma_event_str(cm_event->event));
		        rdma_ack_cm_event(cm_event);
		        return -1;
        }
    } while(1);

	if (rdma_destroy_id(cm_server_id)) {
		rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Server shut-down is complete \n");
	return 0;
}

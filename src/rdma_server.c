#include "rdma_common.h"

#define MAX_CONN (10)

struct s_spin_ctx {
    struct rdma_cm_id* client_id;
    struct ibv_pd* pd;
    struct ibv_comp_channel* comp;
    struct ibv_cq* cq;
    struct ibv_mr* lock_mr;
    struct ibv_mr* server_metadata_mr;
    struct rdma_buffer_attr* server_metadata_attr;
};

uint64_t *lock = NULL;

struct s_spin_ctx* build_server_spin_context(struct rdma_cm_id* client_id) {
    struct s_spin_ctx* ctx;
    struct ibv_pd* pd = NULL;
    struct ibv_comp_channel* comp = NULL;
    struct ibv_cq* cq = NULL;
    struct ibv_mr *lock_mr = NULL;
    struct ibv_mr *server_metadata_mr = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct rdma_buffer_attr *server_metadata_attr;
    struct rdma_conn_param conn_param;

    
    ctx = (struct s_spin_ctx*)malloc(sizeof(struct s_spin_ctx));
    server_metadata_attr = (struct rdma_buffer_attr *)malloc(sizeof(struct rdma_buffer_attr));

    pd = ibv_alloc_pd(client_id->verbs);
    if (!pd) {
        rdma_error("Failed to allocate a protection domain errno: %d\n", -errno);
        free(ctx);
        free(server_metadata_attr);
        return NULL;
    }

    comp = ibv_create_comp_channel(client_id->verbs);
    if(!comp) {
        rdma_error("Failed to create an I/O completion event channel, %d\n", -errno);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        return NULL;
    }

    cq = ibv_create_cq(client_id->verbs, CQ_CAPACITY, NULL, comp, 0);
    if (!cq) {
        rdma_error("Failed to create a completion queue (cq), errno: %d\n", -errno);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        return NULL;
    }

    if (ibv_req_notify_cq(cq,0)) {
        rdma_error("Failed to request notifications on CQ errno: %d \n", -errno);
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
        rdma_error("Failed to create QP due to errno: %d\n", -errno);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        return NULL;
    }

    lock_mr = rdma_buffer_register(pd, lock, sizeof(*lock), (IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_ATOMIC));
    if(!lock_mr){
        rdma_error("Server failed to create lock memory region \n");
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        return NULL;
    }

    (*server_metadata_attr).address = (uint64_t)lock_mr->addr;
    (*server_metadata_attr).length = (uint32_t)lock_mr->length;
    (*server_metadata_attr).stag.remote_stag = (uint32_t)lock_mr->rkey;
    server_metadata_mr = rdma_buffer_register(pd, server_metadata_attr, sizeof(*server_metadata_attr), (IBV_ACCESS_LOCAL_WRITE));
    if(!server_metadata_mr){
        rdma_error("Server failed to create to hold server metadata \n");
        rdma_buffer_free(lock_mr);
        ibv_destroy_cq(cq);
        ibv_destroy_comp_channel(comp);
        ibv_dealloc_pd(pd);
        free(ctx);
        free(server_metadata_attr);
        return NULL;
    }

    (*ctx).client_id = client_id;
    (*ctx).pd = pd;
    (*ctx).comp = comp;
    (*ctx).cq = cq;  
    (*ctx).lock_mr = lock_mr;
    (*ctx).server_metadata_mr = server_metadata_mr;
    (*ctx).server_metadata_attr = server_metadata_attr;
    return ctx;
}

int send_server_metadata(struct s_spin_ctx* ctx) {
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

    if (ibv_post_send((ctx->client_id)->qp, &server_send_wr, &bad_server_send_wr)) {
	    rdma_error("Posting of server metdata failed, errno: %d \n", -errno);
	    return -errno;
    }

    if (process_work_completion_events((ctx->comp), &wc, 1) != 1) {
	    perror("Failed to send server metadata, ret = %d \n");
	    return -1;
    }
    return 0;
}

int clean_up_context(struct s_spin_ctx* ctx) {
    rdma_destroy_qp(ctx->client_id);
	if (rdma_destroy_id(ctx->client_id)) {
	    rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
        return -errno;
	}

    if (ibv_destroy_cq(ctx->cq)) {
        rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
        return -errno;
    }

    if (ibv_destroy_comp_channel(ctx->comp)) {
        rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
        return -errno;
    }
    rdma_buffer_free(ctx->lock_mr);
    rdma_buffer_deregister(ctx->server_metadata_mr);


    if (ibv_dealloc_pd(ctx->pd)) {
        rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
        return -errno;
    }
    free(ctx->server_metadata_attr);
    return 0;
}

struct s_spin_ctx* get_ctx_by_id(struct s_spin_ctx** ctx_arr, struct rdma_cm_id* client_id) {
    struct s_spin_ctx* ret = NULL;
    struct sockaddr_in client_sockaddr;

    memcpy(&client_sockaddr, rdma_get_peer_addr(client_id), sizeof(struct sockaddr_in));
    char* cmp = inet_ntoa(client_sockaddr.sin_addr);
    for(int i = 0; i < MAX_CONN; i++){
        if(ctx_arr[i] != NULL) {
            struct sockaddr_in target_sockaddr;
            memcpy(&target_sockaddr, rdma_get_peer_addr((ctx_arr[i]->client_id)), sizeof(struct sockaddr_in));
            if(strcmp(cmp, inet_ntoa(target_sockaddr.sin_addr)) == 0){
                ret = ctx_arr[i];
                break;
            }
        }
    }
    return ret;
}

struct s_spin_ctx* pop_ctx_by_id(struct s_spin_ctx** ctx_arr, struct rdma_cm_id* client_id) {
    struct s_spin_ctx* ret = NULL;
    struct sockaddr_in client_sockaddr;

    memcpy(&client_sockaddr, rdma_get_peer_addr(client_id), sizeof(struct sockaddr_in));
    char* cmp = inet_ntoa(client_sockaddr.sin_addr);
    for(int i = 0; i < MAX_CONN; i++){
        if(ctx_arr[i] != NULL) {
            struct sockaddr_in target_sockaddr;
            memcpy(&target_sockaddr, rdma_get_peer_addr((ctx_arr[i]->client_id)), sizeof(struct sockaddr_in));
            if(strcmp(cmp, inet_ntoa(target_sockaddr.sin_addr)) == 0){
                ret = ctx_arr[i];
                ctx_arr[i] = NULL;
                break;
            }
        }
    }
    return ret;
}

int main(int argc, char** argv) {
    int option, num_conn = 0;
	struct sockaddr_in server_sockaddr;
    struct rdma_event_channel *cm_event_channel = NULL;
    struct rdma_cm_id *cm_server_id = NULL;
    struct s_spin_ctx** ctx_arr;

    ctx_arr = (struct s_spin_ctx**)malloc(sizeof(struct s_spin_ctx*)*MAX_CONN);

    for (int i = 0; i < MAX_CONN; i++) {
        ctx_arr[i] = NULL;
    }

    lock = calloc(1, sizeof(uint64_t));
    *lock = 0;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	/* Parse Command Line Arguments, not the most reliable code */
	while ((option = getopt(argc, argv, "a:p:")) != -1) {
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
			default:
				break;
		}
	}
	if(!server_sockaddr.sin_port) {
		/* If still zero, that mean no port info provided */
		server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT); /* use default port */
	}

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
        struct s_spin_ctx* ctx = NULL;
    
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
                struct rdma_cm_id* client_id = NULL;
                struct rdma_conn_param conn_param;

                client_id = cm_event->id;

                if (rdma_ack_cm_event(cm_event)) {
                    rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
                    return -errno;
                }

                ctx = build_server_spin_context(client_id);
                if(!ctx) {
                    perror("Failed to build client Context\n");
                    return -1;
                }

                for(int i = 0; i<MAX_CONN ; i++) {
                    if (ctx_arr[i] == NULL) {
                        ctx_arr[i] = ctx;
                        break;
                    }
                }

                memset(&conn_param, 0, sizeof(conn_param));
                conn_param.initiator_depth = 3;
                conn_param.responder_resources = 3;
                if (rdma_accept(ctx->client_id, &conn_param)) {
	                rdma_error("Failed to accept the connection, errno: %d \n", -errno);
	                return -errno;
                }

                num_conn++;
                break;

            case RDMA_CM_EVENT_ESTABLISHED :
                ctx = get_ctx_by_id(ctx_arr, cm_event->id);
                if(!ctx) {
                    perror("Failed to retreive context");
                    return -1;
                }

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return -errno;
	            }

                if(send_server_metadata(ctx)) {
                    perror("Failed to send server metadata \n");
                    return -1;
                }
                break;

            case RDMA_CM_EVENT_DISCONNECTED :
                ctx = pop_ctx_by_id(ctx_arr, cm_event->id);
                if(ctx == NULL) {
                    perror("Failed to retreive context");
                    return -1;
                }

                if (rdma_ack_cm_event(cm_event)) {
		            rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		            return -errno;
	            }

                if (clean_up_context(ctx)) {
                    perror("failed to cleanup client context");
                    return -1;
                }
                free(ctx);
                ctx = NULL;

                num_conn--;
                break;
            default:
                rdma_error("Unexpected event received: %s", rdma_event_str(cm_event->event));
		        rdma_ack_cm_event(cm_event);
		        return -1;
        }
    } while(num_conn > 0);

	if (rdma_destroy_id(cm_server_id)) {
		rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Server shut-down is complete \n");
	return 0;
}

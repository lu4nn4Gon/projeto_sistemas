#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zmq.h>
#include <msgpack.h>

int relogio_logico = 0;

double obter_timestamp() {
    return (double)time(NULL);
}

void atualizar_relogio(int recebido) {
    if (recebido > relogio_logico) {
        relogio_logico = recebido;
    }
}

int incrementar_relogio() {
    relogio_logico++;
    return relogio_logico;
}

// Salva login/canal no arquivo
int salvar(const char *arquivo, const char *nome, double timestamp) {
    FILE *f = fopen(arquivo, "a");

    if (f == NULL) {
        return 0;
    }

    fprintf(f, "%s;%.0f\n", nome, timestamp);
    fclose(f);

    return 1;
}

// Verifica se o canal ja existe
int canal_existe(const char *nome) {
    FILE *f = fopen("canais.txt", "r");

    if (f == NULL) {
        return 0;
    }

    char linha[200];
    char canal[100];

    while (fgets(linha, sizeof(linha), f)) {
        if (sscanf(linha, "%99[^;]", canal) == 1) {
            if (strcmp(canal, nome) == 0) {
                fclose(f);
                return 1;
            }
        }
    }

    fclose(f);
    return 0;
}

//texto no MessagePack
void texto(msgpack_packer *pk, const char *valor) {
    int tamanho = strlen(valor);

    msgpack_pack_str(pk, tamanho);
    msgpack_pack_str_body(pk, valor, tamanho);
}

int consultar_referencia(void *contexto, const char *tipo, const char *nome) {
    void *socket = zmq_socket(contexto, ZMQ_REQ);

    if (socket == NULL) {
        return 0;
    }

    int timeout = 2000;
    int linger = 0;

    zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_setsockopt(socket, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
    zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(linger));

    zmq_connect(socket, "tcp://referencia:6000");

    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);
    msgpack_packer_init(&pk, &buffer, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 2);

    texto(&pk, "tipo");
    texto(&pk, tipo);

    texto(&pk, "nome");
    texto(&pk, nome);

    if (zmq_send(socket, buffer.data, buffer.size, 0) < 0) {
        msgpack_sbuffer_destroy(&buffer);
        zmq_close(socket);
        return 0;
    }

    msgpack_sbuffer_destroy(&buffer);

    zmq_msg_t resposta;
    zmq_msg_init(&resposta);

    int tamanho = zmq_msg_recv(&resposta, socket, 0);

    if (tamanho < 0) {
        zmq_msg_close(&resposta);
        zmq_close(socket);
        return 0;
    }

    msgpack_unpacked resultado;
    msgpack_unpacked_init(&resultado);

    size_t offset = 0;

    if (!msgpack_unpack_next(
            &resultado,
            zmq_msg_data(&resposta),
            (size_t)tamanho,
            &offset)) {

        msgpack_unpacked_destroy(&resultado);
        zmq_msg_close(&resposta);
        zmq_close(socket);
        return 0;
    }

    msgpack_object objeto = resultado.data;

    int rank = 0;
    int sucesso = 0;

    if (objeto.type == MSGPACK_OBJECT_MAP) {
        for (int i = 0; i < objeto.via.map.size; i++) {
            msgpack_object chave = objeto.via.map.ptr[i].key;
            msgpack_object valor = objeto.via.map.ptr[i].val;

            if (chave.type != MSGPACK_OBJECT_STR) {
                continue;
            }

            if (chave.via.str.size == 6 &&
                memcmp(chave.via.str.ptr, "status", 6) == 0 &&
                valor.type == MSGPACK_OBJECT_STR) {

                if (valor.via.str.size == 7 &&
                    memcmp(valor.via.str.ptr, "sucesso", 7) == 0) {
                    sucesso = 1;
                }
            }

            if (chave.via.str.size == 4 &&
                memcmp(chave.via.str.ptr, "rank", 4) == 0 &&
                valor.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {

                rank = (int)valor.via.u64;
            }

            if (chave.via.str.size == 10 &&
                memcmp(chave.via.str.ptr, "servidores", 10) == 0 &&
                valor.type == MSGPACK_OBJECT_ARRAY) {

                printf("Servidores cadastrados:\n");

                for (int j = 0; j < valor.via.array.size; j++) {
                    msgpack_object servidor = valor.via.array.ptr[j];

                    if (servidor.type != MSGPACK_OBJECT_MAP) {
                        continue;
                    }

                    char nome_servidor[100] = "";
                    int rank_servidor = 0;

                    for (int k = 0; k < servidor.via.map.size; k++) {
                        msgpack_object campo = servidor.via.map.ptr[k].key;
                        msgpack_object dado = servidor.via.map.ptr[k].val;

                        if (campo.type != MSGPACK_OBJECT_STR) {
                            continue;
                        }

                        if (campo.via.str.size == 4 &&
                            memcmp(campo.via.str.ptr, "nome", 4) == 0 &&
                            dado.type == MSGPACK_OBJECT_STR) {

                            snprintf(
                                nome_servidor,
                                sizeof(nome_servidor),
                                "%.*s",
                                (int)dado.via.str.size,
                                dado.via.str.ptr
                            );
                        }

                        if (campo.via.str.size == 4 &&
                            memcmp(campo.via.str.ptr, "rank", 4) == 0 &&
                            dado.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {

                            rank_servidor = (int)dado.via.u64;
                        }
                    }

                    printf("  %s - rank %d\n", nome_servidor, rank_servidor);
                }

                fflush(stdout);
            }
        }
    }

    msgpack_unpacked_destroy(&resultado);
    zmq_msg_close(&resposta);
    zmq_close(socket);

    if (!sucesso) {
        return 0;
    }

    if (strcmp(tipo, "rank") == 0) {
        return rank;
    }

    return 1;
}

// salva publicacao
int salvar_publicacao(const char *canal, const char *mensagem, double timestamp) {
    FILE *f = fopen("publicacoes.txt", "a");

    if (f == NULL) {
        return 0;
    }

    fprintf(f, "%.0f;%s;%s\n", timestamp, canal, mensagem);

    int sucesso = !ferror(f);

    if (fclose(f) != 0) {
        sucesso = 0;
    }

    return sucesso;
}

// publica mensagem no proxy
int publicar(void *publicador, const char *canal, const char *mensagem) {
    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);
    msgpack_packer_init(&pk, &buffer, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 4);

    texto(&pk, "canal");
    texto(&pk, canal);

    texto(&pk, "mensagem");
    texto(&pk, mensagem);

    texto(&pk, "timestamp");
    msgpack_pack_double(&pk, obter_timestamp());

    texto(&pk, "relogio_logico");
    msgpack_pack_int(&pk, incrementar_relogio());

    char topico[120];
    snprintf(topico, sizeof(topico), "%s ", canal);

    int resultado = zmq_send(publicador, topico, strlen(topico), ZMQ_SNDMORE);

    if (resultado >= 0) {
        resultado = zmq_send(publicador, buffer.data, buffer.size, 0);
    }

    msgpack_sbuffer_destroy(&buffer);

    return resultado >= 0;
}

// resposta pro bot
void responder(void *socket, const char *status, const char *mensagem, int listar) {
    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);
    msgpack_packer_init(&pk, &buffer, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, listar ? 5 : 4);

    texto(&pk, "status");
    texto(&pk, status);

    texto(&pk, "mensagem");
    texto(&pk, mensagem);

    texto(&pk, "timestamp");
    msgpack_pack_double(&pk, obter_timestamp());

    texto(&pk, "relogio_logico");
    msgpack_pack_int(&pk, incrementar_relogio());

    if (listar) {
        texto(&pk, "canais");

        FILE *f = fopen("canais.txt", "r");
        char linha[200];
        char nome[100];
        int quantidade = 0;

        if (f != NULL) {
            while (fgets(linha, sizeof(linha), f)) {
                if (sscanf(linha, "%99[^;]", nome) == 1) {
                    quantidade++;
                }
            }

            rewind(f);
        }

        msgpack_pack_array(&pk, quantidade);

        if (f != NULL) {
            while (fgets(linha, sizeof(linha), f)) {
                if (sscanf(linha, "%99[^;]", nome) == 1) {
                    texto(&pk, nome);
                }
            }

            fclose(f);
        }
    }

    printf("Servidor enviou: %s - %s | Relogio: %d\n",
           status, mensagem, relogio_logico);
    fflush(stdout);

    zmq_send(socket, buffer.data, buffer.size, 0);

    msgpack_sbuffer_destroy(&buffer);
}

// recebe/ processa as mensagens
void processar(void *socket, void *publicador, char *dados, int tamanho) {
    msgpack_unpacked resultado;
    msgpack_unpacked_init(&resultado);

    if (!msgpack_unpack_next(&resultado, dados, tamanho, NULL)) {
        responder(socket, "erro", "Mensagem invalida", 0);
        msgpack_unpacked_destroy(&resultado);
        return;
    }

    char tipo[50] = "";
    char nome[100] = "";
    char canal[100] = "";
    char mensagem[1024] = "";
    double timestamp = 0;
    int contador_recebido = 0;

    msgpack_object objeto = resultado.data;

    if (objeto.type != MSGPACK_OBJECT_MAP) {
        responder(socket, "erro", "Formato invalido", 0);
        msgpack_unpacked_destroy(&resultado);
        return;
    }

    for (int i = 0; i < objeto.via.map.size; i++) {
        msgpack_object chave = objeto.via.map.ptr[i].key;
        msgpack_object valor = objeto.via.map.ptr[i].val;

        if (chave.type != MSGPACK_OBJECT_STR) {
            continue;
        }

        if (chave.via.str.size == 4 &&
            memcmp(chave.via.str.ptr, "tipo", 4) == 0 &&
            valor.type == MSGPACK_OBJECT_STR) {

            snprintf(tipo, sizeof(tipo), "%.*s",
                     (int)valor.via.str.size, valor.via.str.ptr);
        }

        if (chave.via.str.size == 4 &&
            memcmp(chave.via.str.ptr, "nome", 4) == 0 &&
            valor.type == MSGPACK_OBJECT_STR) {

            snprintf(nome, sizeof(nome), "%.*s",
                     (int)valor.via.str.size, valor.via.str.ptr);
        }

        if (chave.via.str.size == 5 &&
            memcmp(chave.via.str.ptr, "canal", 5) == 0 &&
            valor.type == MSGPACK_OBJECT_STR) {

            snprintf(canal, sizeof(canal), "%.*s",
                     (int)valor.via.str.size, valor.via.str.ptr);
        }

        if (chave.via.str.size == 8 &&
            memcmp(chave.via.str.ptr, "mensagem", 8) == 0 &&
            valor.type == MSGPACK_OBJECT_STR) {

            snprintf(mensagem, sizeof(mensagem), "%.*s",
                     (int)valor.via.str.size, valor.via.str.ptr);
        }

        if (chave.via.str.size == 9 &&
            memcmp(chave.via.str.ptr, "timestamp", 9) == 0 &&
            (valor.type == MSGPACK_OBJECT_FLOAT64 ||
             valor.type == MSGPACK_OBJECT_FLOAT32)) {

            timestamp = valor.via.f64;
        }

        if (chave.via.str.size == 14 &&
            memcmp(chave.via.str.ptr, "relogio_logico", 14) == 0 &&
            (valor.type == MSGPACK_OBJECT_POSITIVE_INTEGER ||
             valor.type == MSGPACK_OBJECT_NEGATIVE_INTEGER)) {

            contador_recebido = (int)valor.via.u64;
        }
    }

    atualizar_relogio(contador_recebido);

    printf("Servidor recebeu: %s - %s | Relogio: %d\n",
           tipo, nome, relogio_logico);
    fflush(stdout);

    //decisoes
    if (timestamp <= 0) {
        responder(socket, "erro", "Timestamp inválido", 0);
    }
    else if (strcmp(tipo, "login") == 0) {
        if (strlen(nome) == 0) {
            responder(socket, "erro", "Nome vazio", 0);
        }
        else if (salvar("logins.txt", nome, timestamp)) {
            responder(socket, "sucesso", "Login realizado com sucesso!!!", 0);
        }
        else {
            responder(socket, "erro", "Erro ao salvar login", 0);
        }
    }
    else if (strcmp(tipo, "criar_canal") == 0) {
        if (strlen(nome) == 0) {
            responder(socket, "erro", "Nome do canal vazio", 0);
        }
        else if (canal_existe(nome)) {
            responder(socket, "erro", "Canal já existe", 0);
        }
        else if (salvar("canais.txt", nome, timestamp)) {
            responder(socket, "sucesso", "Canal criado com sucesso!!!", 0);
        }
        else {
            responder(socket, "erro", "Erro ao criar canal", 0);
        }
    }
    else if (strcmp(tipo, "listar_canais") == 0) {
        responder(socket, "sucesso", "Lista de canais", 1);
    }
    else if (strcmp(tipo, "publicar") == 0) {
        if (strlen(canal) == 0 || strlen(mensagem) == 0) {
            responder(socket, "erro", "Canal ou mensagem vazia", 0);
        }
        else if (!canal_existe(canal)) {
            responder(socket, "erro", "Canal nao existe", 0);
        }
        else if (!salvar_publicacao(canal, mensagem, timestamp)) {
            responder(socket, "erro", "Erro ao salvar publicacao", 0);
        }
        else if (publicar(publicador, canal, mensagem)) {
            responder(socket, "sucesso", "Mensagem publicada", 0);
        }
        else {
            responder(socket, "erro", "Erro ao publicar mensagem", 0);
        }
    }
    else {
        responder(socket, "erro", "funcionalidade desconhecida", 0);
    }

    msgpack_unpacked_destroy(&resultado);
}

int main() {
    void *contexto = zmq_ctx_new();
    void *socket = zmq_socket(contexto, ZMQ_REP);
    void *publicador = zmq_socket(contexto, ZMQ_PUB);

    const char *nome_servidor = getenv("SERVIDOR_NOME");

    if (nome_servidor == NULL || strlen(nome_servidor) == 0) {
        nome_servidor = "servidor";
    }

    int rank = 0;

    while (rank == 0) {
        rank = consultar_referencia(contexto, "rank", nome_servidor);

        if (rank == 0) {
            printf("Aguardando servico de referencia...\n");
            fflush(stdout);
            sleep(2);
        }
    }

    printf("%s cadastrado com rank %d\n", nome_servidor, rank);
    fflush(stdout);

    consultar_referencia(contexto, "listar", nome_servidor);

    char endereco[100];

    sprintf(endereco, "tcp://broker:%s", getenv("SERVIDOR_PORTA"));

    zmq_connect(socket, endereco);
    zmq_connect(publicador, "tcp://proxy:5557");

    printf("Servidor conectado ao broker\n");
    fflush(stdout);

    while (1) {
        zmq_msg_t mensagem;
        zmq_msg_init(&mensagem);

        int tamanho = zmq_msg_recv(&mensagem, socket, 0);

        if (tamanho >= 0) {
            processar(socket, publicador, zmq_msg_data(&mensagem), tamanho);
        }

        zmq_msg_close(&mensagem);
    }

    zmq_close(publicador);
    zmq_close(socket);
    zmq_ctx_destroy(contexto);

    return 0;
}
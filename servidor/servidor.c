#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zmq.h>
#include <msgpack.h>

double obter_timestamp() {
    return (double)time(NULL);
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

// resposta pro bot
void responder(void *socket, const char *status, const char *mensagem, int listar) {
    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);
    msgpack_packer_init(&pk, &buffer, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, listar ? 4 : 3);

    texto(&pk, "status");
    texto(&pk, status);

    texto(&pk, "mensagem");
    texto(&pk, mensagem);

    texto(&pk, "timestamp");
    msgpack_pack_double(&pk, obter_timestamp());

    if (listar) {
        texto(&pk, "canais");

        FILE *f = fopen("canais.txt", "r");
        char linha[200];
        char nome[100];
        int quantidade = 0;

        if (f != NULL) {
            while (fgets(linha, sizeof(linha), f)) {
                quantidade++;
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

    printf("Servidor enviou: %s - %s\n", status, mensagem);
    fflush(stdout);

    zmq_send(socket, buffer.data, buffer.size, 0);

    msgpack_sbuffer_destroy(&buffer);
}

// recebe/ processa as mensagens
void processar(void *socket, char *dados, int tamanho) {
    msgpack_unpacked resultado;
    msgpack_unpacked_init(&resultado);

    if (!msgpack_unpack_next(&resultado, dados, tamanho, NULL)) {
        responder(socket, "erro", "Mensagem invalida", 0);
        msgpack_unpacked_destroy(&resultado);
        return;
    }

    char tipo[50] = "";
    char nome[100] = "";
    double timestamp = 0;

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

        if (chave.via.str.size == 9 &&
            memcmp(chave.via.str.ptr, "timestamp", 9) == 0 &&
            (valor.type == MSGPACK_OBJECT_FLOAT64 ||
             valor.type == MSGPACK_OBJECT_FLOAT32)) {

            timestamp = valor.via.f64;
        }
    }

    printf("Servidor recebeu: %s - %s\n", tipo, nome);
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
    else {
        responder(socket, "erro", "funcionalidade desconhecida", 0);
    }

    msgpack_unpacked_destroy(&resultado);
}

int main() {
    void *contexto = zmq_ctx_new();
    void *socket = zmq_socket(contexto, ZMQ_REP);

    zmq_connect(socket, "tcp://broker:8001");

    printf("Servidor conectado ao broker\n");
    fflush(stdout);

    while (1) {
        zmq_msg_t mensagem;
        zmq_msg_init(&mensagem);

        int tamanho = zmq_msg_recv(&mensagem, socket, 0);

        if (tamanho >= 0) {
            processar(socket, zmq_msg_data(&mensagem), tamanho);
        }

        zmq_msg_close(&mensagem);
    }

    zmq_close(socket);
    zmq_ctx_destroy(contexto);

    return 0;
}

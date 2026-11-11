#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <zmq.h>
#include <msgpack.h>
#include <errno.h>

#define MAX_SERVIDORES 32
#define INTERVALO_ELEICAO 5
#define MAX_REPLICAS_PENDENTES 100
#define INTERVALO_REPLICACAO 2
#define INTERVALO_RECUPERACAO 5

typedef struct {
    char nome[100];
    int rank;
} Servidor;

typedef struct {
    char id[150];
    char canal[100];
    char mensagem[1024];
    double timestamp;

    // Servidores que precisam receber a publicacao
    char servidores_destino[MAX_SERVIDORES][100];
    int total_destinos;

    // Servidores que ja confirmaram o recebimento
    char servidores_confirmados[MAX_SERVIDORES][100];
    int total_confirmados;

} PublicacaoPendente;

typedef struct {
    void *socket;
    int indice_publicacao;
    int indice_servidor;
    char nome_servidor[100];
    int aguardando_resposta;
    time_t inicio_envio;

} EnvioReplica;

static int relogio_logico = 0;
static int mensagens_recebidas = 0;
static double diferenca_relogio = 0;

static unsigned long long contador_publicacoes = 0;
static PublicacaoPendente replicas_pendentes[MAX_REPLICAS_PENDENTES];
static int total_replicas_pendentes = 0;
static time_t ultima_tentativa_replicacao = 0;
static time_t ultima_recuperacao_replicacao = 0;
static int proxima_publicacao_replicacao = 0;
static EnvioReplica envio_atual = {0};

static Servidor servidores[MAX_SERVIDORES];
static int total_servidores = 0;

static char meu_nome[100] = "servidor";
static char coordenador[100] = "";

static int meu_rank = 0;
static int aguardando_coordenador = 0;

static time_t inicio_espera = 0;
static time_t ultima_verificacao = 0;

static void *contexto_global = NULL;
static void *publicador_global = NULL;

// recuperando o contador salvo no disco
static void carregar_contador_publicacoes(void) {
    FILE *f = fopen("contador_publicacoes.txt", "r");

    if (!f) {
        contador_publicacoes = 0;
        return;
    }

    if (fscanf(f, "%llu", &contador_publicacoes) != 1) {
        contador_publicacoes = 0;
    }

    fclose(f);
}

// incrementando/salvando o contador
static int incrementar_contador_publicacoes(void) {
    unsigned long long proximo = contador_publicacoes + 1;

    FILE *f = fopen("contador_publicacoes.txt", "w");

    if (!f) {
        return 0;
    }

    if (fprintf(f, "%llu\n", proximo) < 0) {
        fclose(f);
        return 0;
    }

    if (fclose(f) != 0) {
        return 0;
    }

    contador_publicacoes = proximo;
    return 1;
}

// Pega o horario fisico do servidor com maior precisao
static double obter_timestamp(void) {
    struct timespec agora;

    clock_gettime(CLOCK_REALTIME, &agora);

    return (double)agora.tv_sec +
           (double)agora.tv_nsec / 1000000000.0 +
           diferenca_relogio;
}

// Atualiza o relogio logico
static void atualizar_relogio(int recebido) {
    if (recebido > relogio_logico) {
        relogio_logico = recebido;
    }
}

// Incrementa o relogio logico
static int incrementar_relogio(void) {
    return ++relogio_logico;
}

// Empacota texto em MessagePack
static void texto(msgpack_packer *pk, const char *s) {
    size_t n = strlen(s);

    msgpack_pack_str(pk, n);
    msgpack_pack_str_body(pk, s, n);
}

// Compara uma chave MessagePack
static int chave_igual(msgpack_object chave, const char *s) {
    return chave.type == MSGPACK_OBJECT_STR &&
           chave.via.str.size == strlen(s) &&
           memcmp(chave.via.str.ptr, s, chave.via.str.size) == 0;
}

// Procura um campo na mensagem
static msgpack_object campo(msgpack_object obj, const char *nome) {
    msgpack_object vazio = {0};

    if (obj.type != MSGPACK_OBJECT_MAP) {
        return vazio;
    }

    for (size_t i = 0; i < obj.via.map.size; i++) {
        if (chave_igual(obj.via.map.ptr[i].key, nome)) {
            return obj.via.map.ptr[i].val;
        }
    }

    return vazio;
}

// Le um texto MessagePack
static void ler_texto(
    msgpack_object obj,
    char *destino,
    size_t capacidade
) {
    if (capacidade == 0 || obj.type != MSGPACK_OBJECT_STR) {
        return;
    }

    snprintf(
        destino,
        capacidade,
        "%.*s",
        (int)obj.via.str.size,
        obj.via.str.ptr
    );
}

// Le um inteiro MessagePack
static int ler_inteiro(msgpack_object obj) {
    if (obj.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        return (int)obj.via.u64;
    }

    if (obj.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
        return (int)obj.via.i64;
    }

    return 0;
}

// Le um numero MessagePack
static double ler_numero(msgpack_object obj) {
    if (obj.type == MSGPACK_OBJECT_FLOAT64 ||
        obj.type == MSGPACK_OBJECT_FLOAT32) {
        return obj.via.f64;
    }

    if (obj.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        return (double)obj.via.u64;
    }

    return 0;
}

// Desempacota uma mensagem
static int desempacotar(
    const void *dados,
    size_t tamanho,
    msgpack_unpacked *resultado
) {
    msgpack_unpacked_init(resultado);

    size_t offset = 0;

    return msgpack_unpack_next(
        resultado,
        dados,
        tamanho,
        &offset
    );
}

// Cria socket REQ
static void *novo_req(const char *endereco, int timeout) {
    void *socket = zmq_socket(contexto_global, ZMQ_REQ);

    if (!socket) {
        return NULL;
    }

    int linger = 0;

    zmq_setsockopt(
        socket,
        ZMQ_LINGER,
        &linger,
        sizeof(linger)
    );

    zmq_setsockopt(
        socket,
        ZMQ_RCVTIMEO,
        &timeout,
        sizeof(timeout)
    );

    zmq_setsockopt(
        socket,
        ZMQ_SNDTIMEO,
        &timeout,
        sizeof(timeout)
    );

    if (zmq_connect(socket, endereco) != 0) {
        zmq_close(socket);
        return NULL;
    }

    return socket;
}

// Monta requisicao MessagePack
static void montar_requisicao(
    msgpack_packer *pk,
    const char *tipo,
    const char *nome
) {
    msgpack_pack_map(pk, 4);

    texto(pk, "tipo");
    texto(pk, tipo);

    texto(pk, "nome");
    texto(pk, nome);

    texto(pk, "timestamp");
    msgpack_pack_double(pk, obter_timestamp());

    texto(pk, "relogio_logico");
    msgpack_pack_int(pk, incrementar_relogio());
}

// Envia requisicao e recebe resposta
static int requisitar(
    const char *endereco,
    const char *tipo,
    const char *nome,
    int timeout,
    msgpack_unpacked *resultado
) {
    void *socket = novo_req(endereco, timeout);

    if (!socket) {
        return 0;
    }

    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);

    msgpack_packer_init(
        &pk,
        &buffer,
        msgpack_sbuffer_write
    );

    montar_requisicao(&pk, tipo, nome);

    int enviado = zmq_send(
        socket,
        buffer.data,
        buffer.size,
        0
    );

    msgpack_sbuffer_destroy(&buffer);

    if (enviado < 0) {
        zmq_close(socket);
        return 0;
    }

    zmq_msg_t resposta;
    zmq_msg_init(&resposta);

    int n = zmq_msg_recv(&resposta, socket, 0);

    int ok = n >= 0 &&
             desempacotar(
                 zmq_msg_data(&resposta),
                 (size_t)n,
                 resultado
             );

    zmq_msg_close(&resposta);
    zmq_close(socket);

    if (ok) {
        atualizar_relogio(
            ler_inteiro(
                campo(
                    resultado->data,
                    "relogio_logico"
                )
            )
        );
    }

    return ok;
}

// Consulta o servico de referencia
static int consultar_referencia(const char *tipo) {
    msgpack_unpacked resposta;

    if (!requisitar(
            "tcp://referencia:6000",
            tipo,
            meu_nome,
            2000,
            &resposta
        )) {
        return 0;
    }

    msgpack_object obj = resposta.data;

    char status[40] = "";

    ler_texto(
        campo(obj, "status"),
        status,
        sizeof(status)
    );

    int sucesso = strcmp(status, "sucesso") == 0;
    int rank = ler_inteiro(campo(obj, "rank"));

    if (sucesso && strcmp(tipo, "listar") == 0) {
        msgpack_object lista = campo(obj, "servidores");

        if (lista.type == MSGPACK_OBJECT_ARRAY) {
            total_servidores = 0;

            for (
                size_t i = 0;
                i < lista.via.array.size &&
                total_servidores < MAX_SERVIDORES;
                i++
            ) {
                msgpack_object s = lista.via.array.ptr[i];
                Servidor *dest = &servidores[total_servidores];

                dest->nome[0] = '\0';

                ler_texto(
                    campo(s, "nome"),
                    dest->nome,
                    sizeof(dest->nome)
                );

                dest->rank = ler_inteiro(
                    campo(s, "rank")
                );

                if (dest->nome[0] && dest->rank > 0) {
                    total_servidores++;
                }
            }

            printf("Servidores cadastrados:\n");

            for (int i = 0; i < total_servidores; i++) {
                printf(
                    "  %s - rank %d\n",
                    servidores[i].nome,
                    servidores[i].rank
                );
            }

            fflush(stdout);
        }
    }

    // Na Parte 4, o relogio nao sera sincronizado
    // pelo servico de referencia.
    msgpack_unpacked_destroy(&resposta);

    if (!sucesso) {
        return 0;
    }

    if (strcmp(tipo, "rank") == 0) {
        return rank;
    }

    return 1;
}

// Salva login ou canal
static int salvar(
    const char *arquivo,
    const char *nome,
    double timestamp
) {
    FILE *f = fopen(arquivo, "a");

    if (!f) {
        return 0;
    }

    fprintf(f, "%s;%.0f\n", nome, timestamp);

    return fclose(f) == 0;
}

// Verifica se o canal existe
static int canal_existe(const char *nome) {
    FILE *f = fopen("canais.txt", "r");

    if (!f) {
        return 0;
    }

    char linha[200];
    char canal[100];

    while (fgets(linha, sizeof(linha), f)) {
        if (
            sscanf(linha, "%99[^;]", canal) == 1 &&
            strcmp(canal, nome) == 0
        ) {
            fclose(f);
            return 1;
        }
    }

    fclose(f);
    return 0;
}

// Verifica se uma publicacao foi criada neste servidor
static int publicacao_local(const char *id) {

    size_t tamanho_nome = strlen(meu_nome);

    return strlen(id) > tamanho_nome &&
           strncmp(id, meu_nome, tamanho_nome) == 0 &&
           id[tamanho_nome] == ':';
}

// Verifica se uma publicacao ja foi armazenada
static int publicacao_existe(const char *id) {
    FILE *f = fopen("publicacoes.txt", "r");

    if (!f) {
        // Arquivo ainda nao existe
        if (errno == ENOENT) {
            return 0;
        }

        return -1;
    }

    char *linha = NULL;
    size_t capacidade = 0;
    int existe = 0;

    while (getline(&linha, &capacidade, f) != -1) {
        char *ultimo_separador = strrchr(linha, ';');

        if (!ultimo_separador) {
            continue;
        }

        char *id_salvo = ultimo_separador + 1;
        id_salvo[strcspn(id_salvo, "\r\n")] = '\0';

        if (strcmp(id_salvo, id) == 0) {
            existe = 1;
            break;
        }
    }

    if (ferror(f)) {
        existe = -1;
    }

    free(linha);

    if (fclose(f) != 0) {
        return -1;
    }

    return existe;
}


// salvando publicacao com ID
static int salvar_publicacao(
    const char *canal,
    const char *mensagem,
    double timestamp,
    char *id,
    size_t capacidade_id
) {
    if (!id || capacidade_id == 0) {
        return 0;
    }

    id[0] = '\0';

    if (!incrementar_contador_publicacoes()) {
        return 0;
    }

    int tamanho = snprintf(
        id,
        capacidade_id,
        "%s:%llu",
        meu_nome,
        contador_publicacoes
    );

    if (tamanho < 0 || (size_t)tamanho >= capacidade_id) {
        id[0] = '\0';
        return 0;
    }

    FILE *f = fopen("publicacoes.txt", "a");

    if (!f) {
        return 0;
    }

    int gravado = fprintf(
        f,
        "%.0f;%s;%s;%s\n",
        timestamp,
        canal,
        mensagem,
        id
    );

    int ok = gravado >= 0 && !ferror(f);

    if (fclose(f) != 0) {
        ok = 0;
    }

    if (!ok) {
        id[0] = '\0';
    }

    return ok;
}

// Salva uma publicacao recebida de outro servidor
static int salvar_publicacao_replicada(
    const char *id,
    const char *canal,
    const char *mensagem,
    double timestamp
) {
    int existe = publicacao_existe(id);

    if (existe == 1) {
        // Publicacao ja armazenada
        return 1;
    }

    if (existe == -1) {
        return 0;
    }

    FILE *f = fopen("publicacoes.txt", "a");

    if (!f) {
        return 0;
    }

    int gravado = fprintf(
        f,
        "%.0f;%s;%s;%s\n",
        timestamp,
        canal,
        mensagem,
        id
    );

    int ok = gravado >= 0 && !ferror(f);

    if (fclose(f) != 0) {
        ok = 0;
    }

    return ok;
}

// Identifica os destinos da replicacao. Os dois servidores do compose
// continuam sendo destinos mesmo quando um deles esta temporariamente fora.
static int listar_destinos_replica(char destinos[MAX_SERVIDORES][100]) {
    int total = 0;

    if (strcmp(meu_nome, "servidor1") == 0 ||
        strcmp(meu_nome, "servidor2") == 0) {
        snprintf(destinos[total++], 100, "%s",
                 strcmp(meu_nome, "servidor1") == 0 ? "servidor2" : "servidor1");
    }

    for (int i = 0; i < total_servidores && total < MAX_SERVIDORES; i++) {
        const char *nome = servidores[i].nome;
        if (!nome[0] || strcmp(nome, meu_nome) == 0) continue;
        int repetido = 0;
        for (int j = 0; j < total; j++) {
            if (strcmp(destinos[j], nome) == 0) repetido = 1;
        }
        if (!repetido) snprintf(destinos[total++], 100, "%s", nome);
    }
    return total;
}

// Confirmações persistentes: o par id/destino fica registrado no disco.
static int confirmacao_no_disco(const char *id, const char *destino) {
    FILE *f = fopen("replicacao_confirmacoes.txt", "r");
    if (!f) return 0;
    char *linha = NULL;
    size_t capacidade = 0;
    int confirmado = 0;
    while (getline(&linha, &capacidade, f) != -1) {
        char *separador = strchr(linha, ';');
        if (!separador) continue;
        *separador++ = '\0';
        separador[strcspn(separador, "\r\n")] = '\0';
        if (strcmp(linha, id) == 0 && strcmp(separador, destino) == 0) {
            confirmado = 1;
            break;
        }
    }
    free(linha);
    fclose(f);
    return confirmado;
}

static int salvar_confirmacao_no_disco(const char *id, const char *destino) {
    if (confirmacao_no_disco(id, destino)) return 1;
    FILE *f = fopen("replicacao_confirmacoes.txt", "a");
    if (!f) return 0;
    int ok = fprintf(f, "%s;%s\n", id, destino) >= 0;
    if (fclose(f) != 0) ok = 0;
    return ok;
}

static int replica_esta_na_fila(const char *id) {
    for (int i = 0; i < total_replicas_pendentes; i++) {
        if (strcmp(replicas_pendentes[i].id, id) == 0) return 1;
    }
    return 0;
}

// Adiciona uma publicacao a fila de replicacao
static int adicionar_replica_pendente(
    const char *id,
    const char *canal,
    const char *mensagem,
    double timestamp
) {
    if (total_replicas_pendentes >= MAX_REPLICAS_PENDENTES) {
        return 0;
    }

    PublicacaoPendente *p =
        &replicas_pendentes[total_replicas_pendentes];

    snprintf(p->id, sizeof(p->id), "%s", id);
    snprintf(p->canal, sizeof(p->canal), "%s", canal);
    snprintf(p->mensagem, sizeof(p->mensagem), "%s", mensagem);

    p->timestamp = timestamp;

    memset(p->servidores_destino, 0, sizeof(p->servidores_destino));
    p->total_destinos = listar_destinos_replica(p->servidores_destino);
    if (p->total_destinos == 0) return 0;

    // Inicializa o controle de confirmacoes
    p->total_confirmados = 0;

    memset(
        p->servidores_confirmados,
        0,
        sizeof(p->servidores_confirmados)

    );

    // Restaura as confirmacoes anteriores, inclusive apos reinicializacao.
    for (int i = 0; i < p->total_destinos; i++) {
        if (confirmacao_no_disco(p->id, p->servidores_destino[i])) {
            snprintf(p->servidores_confirmados[p->total_confirmados++],
                     sizeof(p->servidores_confirmados[0]), "%s",
                     p->servidores_destino[i]);
        }
    }

    total_replicas_pendentes++;

    return 1;
}

// Reconstroi a fila a partir das publicacoes locais no disco.
// O limite de 100 vale apenas para a fila em memoria: o restante sera
// carregado em ciclos posteriores, sem descartar publicacoes antigas.
static void recuperar_replicas_do_disco(void) {
    if (envio_atual.socket != NULL ||
        total_replicas_pendentes >= MAX_REPLICAS_PENDENTES) return;

    FILE *f = fopen("publicacoes.txt", "r");
    if (!f) return;
    char *linha = NULL;
    size_t capacidade = 0;
    int recuperadas = 0;

    while (total_replicas_pendentes < MAX_REPLICAS_PENDENTES &&
           getline(&linha, &capacidade, f) != -1) {
        char *primeiro = strchr(linha, ';');
        if (!primeiro) continue;
        char *segundo = strchr(primeiro + 1, ';');
        if (!segundo) continue;
        char *ultimo = strrchr(segundo + 1, ';');
        if (!ultimo || ultimo == segundo) continue;

        *primeiro = '\0';
        *segundo = '\0';
        *ultimo = '\0';
        char *id = ultimo + 1;
        id[strcspn(id, "\r\n")] = '\0';
        if (!publicacao_local(id) || replica_esta_na_fila(id)) continue;

        // Nao reabre publicacoes que ja possuem todas as confirmacoes.
        char destinos[MAX_SERVIDORES][100] = {{0}};
        int total = listar_destinos_replica(destinos);
        if (total == 0) continue;
        int completo = 1;
        for (int j = 0; j < total; j++) {
            if (!confirmacao_no_disco(id, destinos[j])) {
                completo = 0;
                break;
            }
        }
        if (completo) continue;

        char *fim = NULL;
        double timestamp = strtod(linha, &fim);
        if (fim == linha || timestamp <= 0) continue;
        if (adicionar_replica_pendente(id, primeiro + 1,
                                      segundo + 1, timestamp)) {
            recuperadas++;
        }
    }
    free(linha);
    fclose(f);
    if (recuperadas) {
        printf("RECUPERACAO: %d publicacoes adicionadas a fila\n", recuperadas);
        fflush(stdout);
    }
}

// Verifica se um servidor ja confirmou uma publicacao
static int servidor_confirmou_replica(
    PublicacaoPendente *p,
    const char *nome_servidor
) {
    for (int i = 0; i < p->total_confirmados; i++) {
        if (
            strcmp(
                p->servidores_confirmados[i],
                nome_servidor
            ) == 0
        ) {
            return 1;
        }
    }

    return 0;
}

// Registra a confirmacao de uma publicacao
static int registrar_confirmacao_replica(
    PublicacaoPendente *p,
    const char *nome_servidor
) {
    if (servidor_confirmou_replica(p, nome_servidor)) {
        return 1;
    }

    if (p->total_confirmados >= MAX_SERVIDORES) {
        return 0;
    }

    // Nao considera confirmado se nao conseguir persistir no disco.
    if (!salvar_confirmacao_no_disco(p->id, nome_servidor)) return 0;

    snprintf(
        p->servidores_confirmados[p->total_confirmados],
        sizeof(p->servidores_confirmados[0]),
        "%s",
        nome_servidor
    );

    p->total_confirmados++;

    printf(
        "Replicacao de %s confirmada por %s\n",
        p->id,
        nome_servidor
    );

    fflush(stdout);

    return 1;
}

// Remove uma publicacao da fila de replicacao
static void remover_replica_pendente(int indice) {

    if (
        indice < 0 ||
        indice >= total_replicas_pendentes
    ) {
        return;
    }

    printf(
        "Publicacao %s removida da fila de replicacao\n",
        replicas_pendentes[indice].id
    );

    fflush(stdout);

    // Move as publicacoes seguintes uma posicao para tras
    for (
        int i = indice;
        i < total_replicas_pendentes - 1;
        i++
    ) {
        replicas_pendentes[i] = replicas_pendentes[i + 1];
    }

    total_replicas_pendentes--;

    memset(
        &replicas_pendentes[total_replicas_pendentes],
        0,
        sizeof(PublicacaoPendente)
    );
}

// Verifica se todos os destinos confirmaram a publicacao
static int replica_totalmente_confirmada(
    PublicacaoPendente *p
) {
    // Sem destinos registrados, nao remove a publicacao
    if (p->total_destinos == 0) {
        return 0;
    }

    for (int i = 0; i < p->total_destinos; i++) {

        const char *destino = p->servidores_destino[i];

        // Verifica se o destino confirmou
        if (!servidor_confirmou_replica(p, destino)) {
            return 0;
        }
    }

    return 1;
}

// Envia uma publicacao para outro servidor
static int enviar_publicacao(
    const char *destino,
    const char *id,
    const char *canal,
    const char *mensagem,
    double timestamp
) {
    char endereco[180];

    snprintf(
        endereco,
        sizeof(endereco),
        "tcp://%s:6001",
        destino
    );

    void *socket = novo_req(endereco, 1500);

    if (!socket) {
        return 0;
    }

    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);
    msgpack_packer_init(
        &pk,
        &buffer,
        msgpack_sbuffer_write
    );

    msgpack_pack_map(&pk, 7);

    texto(&pk, "tipo");
    texto(&pk, "replicar_publicacao");

    texto(&pk, "nome");
    texto(&pk, meu_nome);

    texto(&pk, "id");
    texto(&pk, id);

    texto(&pk, "canal");
    texto(&pk, canal);

    texto(&pk, "mensagem");
    texto(&pk, mensagem);

    texto(&pk, "timestamp");
    msgpack_pack_double(&pk, timestamp);

    texto(&pk, "relogio_logico");
    msgpack_pack_int(&pk, incrementar_relogio());

    int enviado = zmq_send(
        socket,
        buffer.data,
        buffer.size,
        0
    );

    msgpack_sbuffer_destroy(&buffer);

    if (enviado < 0) {
        zmq_close(socket);
        return 0;
    }

    zmq_msg_t resposta_msg;
    zmq_msg_init(&resposta_msg);

    int n = zmq_msg_recv(
        &resposta_msg,
        socket,
        0
    );

    int ok = 0;

    if (n >= 0) {
        msgpack_unpacked resposta;

        if (desempacotar(
                zmq_msg_data(&resposta_msg),
                (size_t)n,
                &resposta
            )) {
            char status[40] = "";

            ler_texto(
                campo(resposta.data, "status"),
                status,
                sizeof(status)
            );

            atualizar_relogio(
                ler_inteiro(
                    campo(resposta.data, "relogio_logico")
                )
            );

            ok = strcmp(status, "sucesso") == 0;
        }

        msgpack_unpacked_destroy(&resposta);
    }

    zmq_msg_close(&resposta_msg);
    zmq_close(socket);

    return ok;
}

// Encerra uma tentativa de envio de replica

static void encerrar_envio_replica(void) {
    if (envio_atual.socket != NULL) {
        zmq_close(envio_atual.socket);
    }
    
    memset(&envio_atual, 0, sizeof(envio_atual));

}

// Inicia o envio assincrono de uma replica
static int iniciar_envio_replica(
    int indice_publicacao,
    const char *nome_servidor
) {
    if (envio_atual.socket != NULL) {
        return 0;
    }

    if (
        indice_publicacao < 0 ||
        indice_publicacao >= total_replicas_pendentes ||
        !nome_servidor || !nome_servidor[0]
    ) {
        return 0;
    }

    char endereco[180];

    snprintf(
        endereco,
        sizeof(endereco),
        "tcp://%s:6001",
        nome_servidor
    );

    void *socket = novo_req(endereco, 1500);

    if (!socket) {
        return 0;
    }

    envio_atual.socket = socket;

    envio_atual.indice_publicacao = indice_publicacao;
    envio_atual.indice_servidor = -1;

    // Guarda o nome do servidor no momento do envio
    snprintf(
        envio_atual.nome_servidor,
        sizeof(envio_atual.nome_servidor),
        "%s",
        nome_servidor
    );

    envio_atual.aguardando_resposta = 0;
    envio_atual.inicio_envio = time(NULL);

    return 1;
}

// Envia a publicacao sem bloquear o servidor
static int transmitir_replica_pendente(void) {
    if (
        envio_atual.socket == NULL ||
        envio_atual.aguardando_resposta
    ) {
        return 0;
    }

    PublicacaoPendente *p =
        &replicas_pendentes[envio_atual.indice_publicacao];

    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);
    msgpack_packer_init(
        &pk,
        &buffer,
        msgpack_sbuffer_write
    );

    msgpack_pack_map(&pk, 7);

    texto(&pk, "tipo");
    texto(&pk, "replicar_publicacao");

    texto(&pk, "nome");
    texto(&pk, meu_nome);

    texto(&pk, "id");
    texto(&pk, p->id);

    texto(&pk, "canal");
    texto(&pk, p->canal);

    texto(&pk, "mensagem");
    texto(&pk, p->mensagem);

    texto(&pk, "timestamp");
    msgpack_pack_double(&pk, p->timestamp);

    texto(&pk, "relogio_logico");
    msgpack_pack_int(&pk, incrementar_relogio());

    int enviado = zmq_send(
        envio_atual.socket,
        buffer.data,
        buffer.size,
        ZMQ_DONTWAIT
    );

    msgpack_sbuffer_destroy(&buffer);

    if (enviado < 0) {
        return 0;
    }

    envio_atual.aguardando_resposta = 1;
    envio_atual.inicio_envio = time(NULL);

    return 1;
}

// Verifica se o outro servidor confirmou a replicacao
static int verificar_resposta_replica(void) {
    if (
        envio_atual.socket == NULL ||
        !envio_atual.aguardando_resposta
    ) {
        return 0;
    }

    zmq_msg_t resposta_msg;
    zmq_msg_init(&resposta_msg);

    int n = zmq_msg_recv(
        &resposta_msg,
        envio_atual.socket,
        ZMQ_DONTWAIT
    );

    if (n < 0) {
        zmq_msg_close(&resposta_msg);

        if (errno == EAGAIN) {
            return 0;
        }

        encerrar_envio_replica();
        return -1;
    }

    int ok = 0;
    msgpack_unpacked resposta;
    msgpack_unpacked_init(&resposta);

    if (desempacotar(
            zmq_msg_data(&resposta_msg),
            (size_t)n,
            &resposta
        )) {
        char status[40] = "";

        ler_texto(
            campo(resposta.data, "status"),
            status,
            sizeof(status)
        );

        atualizar_relogio(
            ler_inteiro(
                campo(resposta.data, "relogio_logico")
            )
        );

        ok = strcmp(status, "sucesso") == 0;
    }

    msgpack_unpacked_destroy(&resposta);
    zmq_msg_close(&resposta_msg);

    if (!ok) {
        encerrar_envio_replica();
        return -1;
    }

    // Verifica se a publicacao ainda existe na fila
    if (
        envio_atual.indice_publicacao < 0 ||
        envio_atual.indice_publicacao >= total_replicas_pendentes
    ) {
        encerrar_envio_replica();
        return -1;
    }

    // Registra qual servidor confirmou a publicacao
    PublicacaoPendente *p =
        &replicas_pendentes[envio_atual.indice_publicacao];

    // Usa o nome armazenado no momento do envio
    const char *nome_servidor =
        envio_atual.nome_servidor;

    if (!registrar_confirmacao_replica(p, nome_servidor)) {
        encerrar_envio_replica();
        return -1;
    }

    // Finaliza a tentativa apos receber a confirmacao
    encerrar_envio_replica();

    return 1;
}



// Verifica se a tentativa de replicacao expirou
static int verificar_timeout_replica(void) {
    if (envio_atual.socket == NULL) {
        return 0;
    }

    time_t agora = time(NULL);

    if (
        difftime(agora, envio_atual.inicio_envio)
        >= INTERVALO_REPLICACAO
    ) {
        printf(
            "Timeout na replicacao para %s\n",
            envio_atual.nome_servidor
        );

        encerrar_envio_replica();

        return 1;
    }

    return 0;
}

// Procura uma publicacao pendente para replicar
static void iniciar_proxima_replica(void) {
    if (envio_atual.socket != NULL || total_replicas_pendentes == 0) return;

    time_t agora = time(NULL);
    if (difftime(agora, ultima_tentativa_replicacao) < INTERVALO_REPLICACAO)
        return;
    ultima_tentativa_replicacao = agora;

    // Alterna as publicacoes para nao bloquear a fila se um destino cair.
    for (int n = 0; n < total_replicas_pendentes; n++) {
        int i = (proxima_publicacao_replicacao + n) % total_replicas_pendentes;
        PublicacaoPendente *p = &replicas_pendentes[i];
        for (int j = 0; j < p->total_destinos; j++) {
            const char *destino = p->servidores_destino[j];
            if (servidor_confirmou_replica(p, destino)) continue;
            proxima_publicacao_replicacao = (i + 1) % total_replicas_pendentes;
            if (iniciar_envio_replica(i, destino)) {
                if (!transmitir_replica_pendente()) encerrar_envio_replica();
            }
            return;
        }
    }
}

// Remove publicacoes confirmadas quando nao existe envio ativo
static void limpar_replicas_confirmadas(void) {

    // Nao altera a fila durante um envio
    if (envio_atual.socket != NULL) {
        return;
    }

    for (int i = 0; i < total_replicas_pendentes;) {

        PublicacaoPendente *p = &replicas_pendentes[i];

        if (replica_totalmente_confirmada(p)) {
            remover_replica_pendente(i);
        } else {
            i++;
        }
    }
}

// Publica mensagem no proxy
static int publicar(
    void *publicador,
    const char *canal,
    const char *mensagem
) {
    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);

    msgpack_packer_init(
        &pk,
        &buffer,
        msgpack_sbuffer_write
    );

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

    snprintf(
        topico,
        sizeof(topico),
        "%s ",
        canal
    );

    int ok =
        zmq_send(
            publicador,
            topico,
            strlen(topico),
            ZMQ_SNDMORE
        ) >= 0 &&
        zmq_send(
            publicador,
            buffer.data,
            buffer.size,
            0
        ) >= 0;

    msgpack_sbuffer_destroy(&buffer);

    return ok;
}

// Envia resposta
static void responder(
    void *socket,
    const char *status,
    const char *mensagem,
    int listar
) {
    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);

    msgpack_packer_init(
        &pk,
        &buffer,
        msgpack_sbuffer_write
    );

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

        if (f) {
            while (fgets(linha, sizeof(linha), f)) {
                if (sscanf(linha, "%99[^;]", nome) == 1) {
                    quantidade++;
                }
            }

            rewind(f);
        }

        msgpack_pack_array(&pk, quantidade);

        if (f) {
            while (fgets(linha, sizeof(linha), f)) {
                if (sscanf(linha, "%99[^;]", nome) == 1) {
                    texto(&pk, nome);
                }
            }

            fclose(f);
        }
    }

    printf(
        "Servidor enviou: %s - %s | Relogio: %d\n",
        status,
        mensagem,
        relogio_logico
    );

    fflush(stdout);

    zmq_send(
        socket,
        buffer.data,
        buffer.size,
        0
    );

    msgpack_sbuffer_destroy(&buffer);
}

// Processa mensagens dos bots
static void processar(
    void *socket,
    void *publicador,
    const void *dados,
    size_t tamanho
) {
    msgpack_unpacked resultado;

    if (!desempacotar(dados, tamanho, &resultado)) {
        responder(socket, "erro", "Mensagem invalida", 0);
        msgpack_unpacked_destroy(&resultado);
        return;
    }

    msgpack_object obj = resultado.data;

    if (obj.type != MSGPACK_OBJECT_MAP) {
        responder(socket, "erro", "Formato invalido", 0);
        msgpack_unpacked_destroy(&resultado);
        return;
    }

    char tipo[50] = "";
    char nome[100] = "";
    char canal[100] = "";
    char mensagem[1024] = "";
    char id_publicacao[150] = "";

    ler_texto(
        campo(obj, "tipo"),
        tipo,
        sizeof(tipo)
    );

    ler_texto(
        campo(obj, "nome"),
        nome,
        sizeof(nome)
    );

    ler_texto(
        campo(obj, "canal"),
        canal,
        sizeof(canal)
    );

    ler_texto(
        campo(obj, "mensagem"),
        mensagem,
        sizeof(mensagem)
    );

    double timestamp = ler_numero(
        campo(obj, "timestamp")
    );

    atualizar_relogio(
        ler_inteiro(
            campo(obj, "relogio_logico")
        )
    );

    printf(
        "Servidor recebeu: %s - %s | Relogio: %d\n",
        tipo,
        nome,
        relogio_logico
    );

    fflush(stdout);

    if (timestamp <= 0) {
        responder(
            socket,
            "erro",
            "Timestamp invalido",
            0
        );
    }

    // Login
    else if (strcmp(tipo, "login") == 0) {
        if (!nome[0]) {
            responder(
                socket,
                "erro",
                "Nome vazio",
                0
            );
        }
        else if (salvar("logins.txt", nome, timestamp)) {
            responder(
                socket,
                "sucesso",
                "Login realizado com sucesso!!!",
                0
            );
        }
        else {
            responder(
                socket,
                "erro",
                "Erro ao salvar login",
                0
            );
        }
    }

    // Criar canal
    else if (strcmp(tipo, "criar_canal") == 0) {
        if (!nome[0]) {
            responder(
                socket,
                "erro",
                "Nome do canal vazio",
                0
            );
        }
        else if (canal_existe(nome)) {
            responder(
                socket,
                "erro",
                "Canal ja existe",
                0
            );
        }
        else if (salvar("canais.txt", nome, timestamp)) {
            responder(
                socket,
                "sucesso",
                "Canal criado com sucesso!!!",
                0
            );
        }
        else {
            responder(
                socket,
                "erro",
                "Erro ao criar canal",
                0
            );
        }
    }

    // Listar canais
    else if (strcmp(tipo, "listar_canais") == 0) {
        responder(
            socket,
            "sucesso",
            "Lista de canais",
            1
        );
    }

    // Publicar mensagem
    else if (strcmp(tipo, "publicar") == 0) {
        if (!canal[0] || !mensagem[0]) {
            responder(
                socket,
                "erro",
                "Canal ou mensagem vazia",
                0
            );
        }
        else if (!canal_existe(canal)) {

    responder(

        socket,

        "erro",

        "Canal nao existe",

        0

    );

}

        else if ( total_replicas_pendentes >= MAX_REPLICAS_PENDENTES) {
            responder(
                socket,
                "erro",
                "Fila de replicacao cheia. Tente novamente.",
                0
            );

        }
        else if (!salvar_publicacao(
                     canal,
                     mensagem,
                     timestamp,
                     id_publicacao,
                     sizeof(id_publicacao)
                 )) {
            responder(
                socket,
                "erro",
                "Erro ao salvar publicacao",
                0
            );
        }
        else {
            // Primeiro coloca na fila; o arquivo em disco permite recuperar
            // mesmo se houver falha entre a gravacao e este ponto.
            if (!adicionar_replica_pendente(
                    id_publicacao, canal, mensagem, timestamp)) {
                responder(socket, "erro",
                          "Publicacao salva; replicacao pendente de recuperacao", 0);
            }
            else if (publicar(publicador, canal, mensagem)) {
                printf("Publicacao %s adicionada a fila de replicacao\n",
                       id_publicacao);
                fflush(stdout);
                responder(socket, "sucesso", "Mensagem publicada", 0);
            }
            else {
                responder(socket, "erro", "Erro ao publicar mensagem", 0);
            }
        }
    }

    // Funcionalidade desconhecida
    else {
        responder(
            socket,
            "erro",
            "funcionalidade desconhecida",
            0
        );
    }

    msgpack_unpacked_destroy(&resultado);
}

// Anuncia o coordenador eleito
static void anunciar_coordenador(void) {
    strncpy(
        coordenador,
        meu_nome,
        sizeof(coordenador) - 1
    );

    coordenador[sizeof(coordenador) - 1] = '\0';
    aguardando_coordenador = 0;

    // Publica no topico servers
    publicar(
        publicador_global,
        "servers",
        coordenador
    );

    printf(
        "COORDENADOR ELEITO: %s (rank %d)\n",
        coordenador,
        meu_rank
    );

    fflush(stdout);
}

// Consulta outro servidor
static int consultar_par(
    const char *nome,
    const char *tipo
) {
    char endereco[180];

    snprintf(
        endereco,
        sizeof(endereco),
        "tcp://%s:6001",
        nome
    );

    msgpack_unpacked resposta;

    if (!requisitar(
            endereco,
            tipo,
            meu_nome,
            700,
            &resposta
        )) {
        return 0;
    }

    char status[40] = "";
    char mensagem[40] = "";

    ler_texto(
        campo(resposta.data, "status"),
        status,
        sizeof(status)
    );

    ler_texto(
        campo(resposta.data, "mensagem"),
        mensagem,
        sizeof(mensagem)
    );

    int ok =
        strcmp(status, "sucesso") == 0 &&
        strcmp(mensagem, "OK") == 0;

    msgpack_unpacked_destroy(&resposta);

    return ok;
}

// Envia a correcao do relogio para outro servidor
static int enviar_ajuste(
    const char *nome,
    double ajuste
) {
    char endereco[180];

    snprintf(
        endereco,
        sizeof(endereco),
        "tcp://%s:6001",
        nome
    );

    void *socket = novo_req(endereco, 1500);

    if (!socket) {
        return 0;
    }

    msgpack_sbuffer buffer;
    msgpack_packer pk;

    msgpack_sbuffer_init(&buffer);

    msgpack_packer_init(
        &pk,
        &buffer,
        msgpack_sbuffer_write
    );

    msgpack_pack_map(&pk, 5);

    texto(&pk, "tipo");
    texto(&pk, "ajustar_relogio");

    texto(&pk, "nome");
    texto(&pk, meu_nome);

    texto(&pk, "timestamp");
    msgpack_pack_double(&pk, obter_timestamp());

    texto(&pk, "relogio_logico");
    msgpack_pack_int(&pk, incrementar_relogio());

    texto(&pk, "ajuste");
    msgpack_pack_double(&pk, ajuste);

    int enviado = zmq_send(
        socket,
        buffer.data,
        buffer.size,
        0
    );

    msgpack_sbuffer_destroy(&buffer);

    if (enviado < 0) {
        zmq_close(socket);
        return 0;
    }

    zmq_msg_t mensagem;
    zmq_msg_init(&mensagem);

    int n = zmq_msg_recv(
        &mensagem,
        socket,
        0
    );

    int ok = 0;

    if (n >= 0) {
        msgpack_unpacked resposta;

        if (desempacotar(
                zmq_msg_data(&mensagem),
                (size_t)n,
                &resposta
            )) {
            char status[40] = "";

            ler_texto(
                campo(resposta.data, "status"),
                status,
                sizeof(status)
            );

            atualizar_relogio(
                ler_inteiro(
                    campo(
                        resposta.data,
                        "relogio_logico"
                    )
                )
            );

            ok = strcmp(status, "sucesso") == 0;
        }

        msgpack_unpacked_destroy(&resposta);
    }

    zmq_msg_close(&mensagem);
    zmq_close(socket);

    return ok;
}

// Consulta os relogios e calcula a media de Berkeley
static void calcular_media_berkeley(void) {
    // Apenas o coordenador realiza o calculo
    if (strcmp(coordenador, meu_nome) != 0) {
        return;
    }

    // Atualiza a lista de servidores
    if (!consultar_referencia("listar")) {
        printf("BERKELEY: erro ao consultar referencia\n");
        fflush(stdout);
        return;
    }

    double soma_diferencas = 0.0;
    int participantes = 1;

    // Guarda os desvios para calcular os ajustes
    double desvios[MAX_SERVIDORES] = {0};
    int ativos[MAX_SERVIDORES] = {0};

    printf(
        "BERKELEY: coordenador %s iniciou sincronizacao\n",
        meu_nome
    );

    printf(
        "BERKELEY: horario do coordenador: %.6f\n",
        obter_timestamp()
    );

    // Consulta os outros servidores
    for (int i = 0; i < total_servidores; i++) {
        if (strcmp(servidores[i].nome, meu_nome) == 0) {
            continue;
        }

        char endereco[180];

        snprintf(
            endereco,
            sizeof(endereco),
            "tcp://%s:6001",
            servidores[i].nome
        );

        msgpack_unpacked resposta;

        double inicio = obter_timestamp();

        if (!requisitar(
                endereco,
                "consultar_relogio",
                meu_nome,
                700,
                &resposta
            )) {
            printf(
                "BERKELEY: %s nao respondeu\n",
                servidores[i].nome
            );
            continue;
        }

        double fim = obter_timestamp();

        char status[40] = "";

        ler_texto(
            campo(resposta.data, "status"),
            status,
            sizeof(status)
        );

        double horario_remoto = ler_numero(
            campo(resposta.data, "timestamp")
        );

        if (
            strcmp(status, "sucesso") == 0 &&
            horario_remoto > 0
        ) {
            // Considera metade do tempo de ida e volta
            double latencia = (fim - inicio) / 2.0;

            double horario_estimado =
                horario_remoto + latencia;

            double diferenca =
                horario_estimado - fim;

            soma_diferencas += diferenca;
            participantes++;

            // Guarda o desvio deste servidor
            desvios[i] = diferenca;
            ativos[i] = 1;

            printf(
                "BERKELEY: %s | diferenca: %.6f segundos\n",
                servidores[i].nome,
                diferenca
            );
        }

        msgpack_unpacked_destroy(&resposta);
    }

    // Calcula a media incluindo o coordenador
    double media_diferencas =
        soma_diferencas / participantes;

    double horario_medio =
        obter_timestamp() + media_diferencas;

    printf(
        "BERKELEY: participantes: %d\n",
        participantes
    );

    printf(
        "BERKELEY: media dos desvios: %.6f segundos\n",
        media_diferencas
    );

    printf(
        "BERKELEY: horario medio: %.6f\n",
        horario_medio
    );

    // Envia os ajustes aos outros servidores
    for (int i = 0; i < total_servidores; i++) {
        if (!ativos[i]) {
            continue;
        }

        double ajuste =
            media_diferencas - desvios[i];

        if (enviar_ajuste(
                servidores[i].nome,
                ajuste
            )) {
            printf(
                "BERKELEY: ajuste enviado a %s: %+.6f segundos\n",
                servidores[i].nome,
                ajuste
            );
        }
        else {
            printf(
                "BERKELEY: falha ao ajustar %s\n",
                servidores[i].nome
            );
        }
    }

    // Ajusta o relogio do proprio coordenador
    diferenca_relogio += media_diferencas;

    printf(
        "BERKELEY: coordenador %s ajustou seu relogio em %+.6f segundos\n",
        meu_nome,
        media_diferencas
    );

    fflush(stdout);
}

// Inicia eleicao pelo algoritmo Bully
static void iniciar_eleicao(void) {
    // Atualiza lista de servidores e ranks
    consultar_referencia("listar");

    int maior_respondeu = 0;

    printf(
        "%s iniciou eleicao (rank %d)\n",
        meu_nome,
        meu_rank
    );

    fflush(stdout);

    // Consulta servidores com rank maior
    for (int i = 0; i < total_servidores; i++) {
        if (
            servidores[i].rank <= meu_rank ||
            strcmp(servidores[i].nome, meu_nome) == 0
        ) {
            continue;
        }

        if (consultar_par(
                servidores[i].nome,
                "eleicao"
            )) {
            printf(
                "%s recebeu OK de %s\n",
                meu_nome,
                servidores[i].nome
            );

            fflush(stdout);
            maior_respondeu = 1;
        }
    }

    // Se nenhum servidor maior respondeu,
    // este servidor vira coordenador
    if (!maior_respondeu) {
        anunciar_coordenador();
    }
    else {
        coordenador[0] = '\0';
        aguardando_coordenador = 1;
        inicio_espera = time(NULL);

        printf(
            "%s aguardando anuncio do coordenador\n",
            meu_nome
        );

        fflush(stdout);
    }
}

// Processa mensagens diretas de outros servidores
static void processar_servidor(
    void *socket,
    const void *dados,
    size_t tamanho,
    int *pediram_eleicao
) {
    msgpack_unpacked resultado;

    if (!desempacotar(dados, tamanho, &resultado)) {
        responder(
            socket,
            "erro",
            "Mensagem invalida",
            0
        );

        msgpack_unpacked_destroy(&resultado);
        return;
    }

    char tipo[50] = "";
    char nome[100] = "";

    ler_texto(
        campo(resultado.data, "tipo"),
        tipo,
        sizeof(tipo)
    );

    ler_texto(
        campo(resultado.data, "nome"),
        nome,
        sizeof(nome)
    );

    atualizar_relogio(
        ler_inteiro(
            campo(
                resultado.data,
                "relogio_logico"
            )
        )
    );

    printf(
        "Mensagem direta recebida: %s de %s | Relogio: %d\n",
        tipo,
        nome,
        relogio_logico
    );

    fflush(stdout);

    if (strcmp(tipo, "eleicao") == 0) {
        responder(
            socket,
            "sucesso",
            "OK",
            0
        );

        // Inicia a propria eleicao apos responder
        if (strcmp(coordenador, meu_nome) == 0) {
            anunciar_coordenador();
        }
        else {
            *pediram_eleicao = 1;
        }
    }
    else if (strcmp(tipo, "ping") == 0) {
        responder(
            socket,
            "sucesso",
            "OK",
            0
        );
    }
    else if (strcmp(tipo, "consultar_relogio") == 0) {
        responder(
            socket,
            "sucesso",
            "Horario do servidor",
            0
        );
    }
    else if (strcmp(tipo, "ajustar_relogio") == 0) {
        // Apenas o coordenador pode enviar ajustes
        if (
            strcmp(nome, coordenador) != 0 ||
            strcmp(nome, meu_nome) == 0
        ) {
            responder(
                socket,
                "erro",
                "Somente coordenador pode ajustar",
                0
            );
        }
        else {
            msgpack_object valor = campo(
                resultado.data,
                "ajuste"
            );

            if (
                valor.type != MSGPACK_OBJECT_FLOAT64 &&
                valor.type != MSGPACK_OBJECT_FLOAT32
            ) {
                responder(
                    socket,
                    "erro",
                    "Ajuste invalido",
                    0
                );
            }
            else {
                double ajuste = ler_numero(valor);

                // Aplica o ajuste no relogio fisico
                diferenca_relogio += ajuste;

                printf(
                    "BERKELEY: %s recebeu ajuste de %s: %+.6f segundos\n",
                    meu_nome,
                    nome,
                    ajuste
                );

                fflush(stdout);

                responder(
                    socket,
                    "sucesso",
                    "Relogio ajustado",
                    0
                );
            }
        }
    }
    else if (strcmp(tipo, "replicar_publicacao") == 0) {
        char id[150] = "";
        char canal[100] = "";
        char mensagem[1024] = "";

        ler_texto(
            campo(resultado.data, "id"),
            id,
            sizeof(id)
        );

        ler_texto(
            campo(resultado.data, "canal"),
            canal,
            sizeof(canal)
        );

        ler_texto(
            campo(resultado.data, "mensagem"),
            mensagem,
            sizeof(mensagem)
        );

        double timestamp = ler_numero(
            campo(resultado.data, "timestamp")
        );

        if (!id[0] || !canal[0] ||
            !mensagem[0] || timestamp <= 0) {
            responder(
                socket,
                "erro",
                "Publicacao replicada invalida",
                0
            );
        }
        else if (salvar_publicacao_replicada(
                     id,
                     canal,
                     mensagem,
                     timestamp
                 )) {
            responder(
                socket,
                "sucesso",
                "Publicacao replicada",
                0
            );
        }
        else {
            responder(
                socket,
                "erro",
                "Erro ao salvar replica",
                0
            );
        }
    }
    else {
        responder(
            socket,
            "erro",
            "Tipo desconhecido",
            0
        );
    }

    msgpack_unpacked_destroy(&resultado);
}

// Recebe anuncio do coordenador
static void receber_anuncio(void *assinante) {
    zmq_msg_t topico;
    zmq_msg_t dados;

    zmq_msg_init(&topico);
    zmq_msg_init(&dados);

    if (
        zmq_msg_recv(&topico, assinante, 0) >= 0 &&
        zmq_msg_recv(&dados, assinante, 0) >= 0
    ) {
        msgpack_unpacked resultado;

        if (desempacotar(
                zmq_msg_data(&dados),
                zmq_msg_size(&dados),
                &resultado
            )) {
            char eleito[100] = "";

            ler_texto(
                campo(resultado.data, "mensagem"),
                eleito,
                sizeof(eleito)
            );

            atualizar_relogio(
                ler_inteiro(
                    campo(
                        resultado.data,
                        "relogio_logico"
                    )
                )
            );

            if (eleito[0]) {
                snprintf(
                    coordenador,
                    sizeof(coordenador),
                    "%s",
                    eleito
                );

                aguardando_coordenador = 0;

                printf(
                    "%s reconheceu coordenador: %s\n",
                    meu_nome,
                    coordenador
                );

                fflush(stdout);
            }

            msgpack_unpacked_destroy(&resultado);
        }
    }

    zmq_msg_close(&topico);
    zmq_msg_close(&dados);
}

int main(void) {
     // recuperando o contador persistente
    carregar_contador_publicacoes();
    contexto_global = zmq_ctx_new();

    if (!contexto_global) {
        return 1;
    }

    // Comunicacao com broker
    void *socket = zmq_socket(
        contexto_global,
        ZMQ_REP
    );

    // Publicacao de mensagens
    publicador_global = zmq_socket(
        contexto_global,
        ZMQ_PUB
    );

    // Comunicacao direta entre servidores
    void *socket_servidores = zmq_socket(
        contexto_global,
        ZMQ_REP
    );

    // Recebe anuncios do coordenador
    void *assinante = zmq_socket(
        contexto_global,
        ZMQ_SUB
    );

    if (
        !socket ||
        !publicador_global ||
        !socket_servidores ||
        !assinante
    ) {
        return 1;
    }

    // Porta para comunicacao entre servidores
    if (zmq_bind(
            socket_servidores,
            "tcp://*:6001"
        ) != 0) {
        perror("Erro ao abrir porta 6001");
        return 1;
    }

    // Nome do servidor
    const char *nome = getenv("SERVIDOR_NOME");

    if (nome && nome[0]) {
        snprintf(
            meu_nome,
            sizeof(meu_nome),
            "%s",
            nome
        );
    }

    // Cadastro no servico de referencia
    while (!meu_rank) {
        meu_rank = consultar_referencia("rank");

        if (!meu_rank) {
            printf(
                "Aguardando servico de referencia...\n"
            );

            fflush(stdout);
            sleep(2);
        }
    }

    printf(
        "%s cadastrado com rank %d\n",
        meu_nome,
        meu_rank
    );

    fflush(stdout);

    consultar_referencia("listar");
    recuperar_replicas_do_disco();
    ultima_recuperacao_replicacao = time(NULL);

    // Conecta ao broker
    const char *porta = getenv("SERVIDOR_PORTA");
    char endereco[100];

    snprintf(
        endereco,
        sizeof(endereco),
        "tcp://broker:%s",
        porta ? porta : "8001"
    );

    zmq_connect(socket, endereco);

    // Conecta ao proxy
    zmq_connect(
        publicador_global,
        "tcp://proxy:5557"
    );

    // Inscreve no topico servers
    zmq_connect(
        assinante,
        "tcp://proxy:5558"
    );

    zmq_setsockopt(
        assinante,
        ZMQ_SUBSCRIBE,
        "servers ",
        strlen("servers ")
    );

    printf("Servidor conectado ao broker\n");

    printf(
        "Servidor aguardando mensagens diretas na porta 6001\n"
    );

    fflush(stdout);

    // Aguarda a conexao PUB/SUB
    sleep(2);

    // Primeira eleicao
    iniciar_eleicao();

    ultima_verificacao = time(NULL);

    // Monitora broker, servidores e anuncios
    zmq_pollitem_t itens[] = {
        {socket, 0, ZMQ_POLLIN, 0},
        {socket_servidores, 0, ZMQ_POLLIN, 0},
        {assinante, 0, ZMQ_POLLIN, 0}
    };

    while (1) {
        int eventos = zmq_poll(
            itens,
            3,
            500
        );

        if (eventos < 0) {
            continue;
        }

        int pediram_eleicao = 0;

        // Mensagens recebidas do broker
        if (itens[0].revents & ZMQ_POLLIN) {
            zmq_msg_t mensagem;
            zmq_msg_init(&mensagem);

            int n = zmq_msg_recv(
                &mensagem,
                socket,
                0
            );

            if (n >= 0) {
                processar(
                    socket,
                    publicador_global,
                    zmq_msg_data(&mensagem),
                    (size_t)n
                );

                mensagens_recebidas++;

                // Heartbeat a cada 10 mensagens
                if (mensagens_recebidas % 10 == 0) {
                    int ok = consultar_referencia(
                        "heartbeat"
                    );

                    if (ok) {
                        printf(
                            "%s enviou heartbeat apos %d mensagens - OK\n",
                            meu_nome,
                            mensagens_recebidas
                        );
                    }
                    else {
                        printf(
                            "%s nao conseguiu enviar heartbeat. Tentando recadastro...\n",
                            meu_nome
                        );

                        int novo_rank = consultar_referencia(
                            "rank"
                        );

                        if (novo_rank) {
                            meu_rank = novo_rank;
                        }
                    }

                    fflush(stdout);
                }

                // Berkeley: calcula a media a cada 15 mensagens
                // recebidas pelo coordenador
                if (
                    mensagens_recebidas % 15 == 0 &&
                    strcmp(coordenador, meu_nome) == 0
                ) {
                    calcular_media_berkeley();
                }
            }

            zmq_msg_close(&mensagem);
        }

        // Mensagens diretas de outros servidores
        if (itens[1].revents & ZMQ_POLLIN) {
            zmq_msg_t mensagem;
            zmq_msg_init(&mensagem);

            int n = zmq_msg_recv(
                &mensagem,
                socket_servidores,
                0
            );

            if (n >= 0) {
                processar_servidor(
                    socket_servidores,
                    zmq_msg_data(&mensagem),
                    (size_t)n,
                    &pediram_eleicao
                );
            }

            zmq_msg_close(&mensagem);
        }

        // Anuncios recebidos no topico servers
        if (itens[2].revents & ZMQ_POLLIN) {
            receber_anuncio(assinante);
        }

        // Outro servidor solicitou eleicao
        if (
            pediram_eleicao &&
            strcmp(coordenador, meu_nome) != 0
        ) {
            iniciar_eleicao();
        }

        time_t agora = time(NULL);

        // Verifica coordenador periodicamente
        if (
            agora - ultima_verificacao >=
            INTERVALO_ELEICAO
        ) {
            ultima_verificacao = agora;

            // Aguarda anuncio do coordenador
            if (
                aguardando_coordenador &&
                agora - inicio_espera >= 3
            ) {
                iniciar_eleicao();
            }

            // Verifica se o coordenador esta ativo
            else if (
                !aguardando_coordenador &&
                coordenador[0] &&
                strcmp(coordenador, meu_nome) != 0
            ) {
                if (!consultar_par(
                        coordenador,
                        "ping"
                    )) {
                    printf(
                        "Coordenador %s indisponivel!\n",
                        coordenador
                    );

                    fflush(stdout);

                    iniciar_eleicao();
                }
            }

           else if (

                !coordenador[0] &&

                !aguardando_coordenador

            ) {

                iniciar_eleicao();

            }

        }

        // Verifica se existe uma replicacao em andamento

        if (envio_atual.socket != NULL) {

            // Verifica se o outro servidor confirmou

            if (envio_atual.aguardando_resposta) {

                verificar_resposta_replica();

            }

            // Encerra tentativas que demoraram demais

            verificar_timeout_replica();

        }


        // Remove publicacoes que ja foram confirmadas
        limpar_replicas_confirmadas();
        if (difftime(time(NULL), ultima_recuperacao_replicacao) >=
            INTERVALO_RECUPERACAO) {
            ultima_recuperacao_replicacao = time(NULL);
            recuperar_replicas_do_disco();
        }
        // Procura publicacoes pendentes para enviar
        iniciar_proxima_replica();



    }

    zmq_close(assinante);
    zmq_close(socket_servidores);
    zmq_close(publicador_global);
    zmq_close(socket);
    zmq_ctx_destroy(contexto_global);

    return 0;
}
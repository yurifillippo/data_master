CREATE TABLE IF NOT EXISTS produtos_bancarios (
    produto_id INT PRIMARY KEY,
    nome VARCHAR(255),
    descricao TEXT,
    categoria VARCHAR(100),
    data_lancamento DATE,
    taxa_juros DECIMAL(5, 2),
    taxa_administracao DECIMAL(5, 2),
    limite_credito DECIMAL(10, 2),
    prazo INT,
    contato_suporte VARCHAR(255),
    taxa_adesao DECIMAL(10, 2),
    data_ultima_atualizacao DATE,
    prazo_carencia INT,
    taxa_rentabilidade DECIMAL(5, 2),
    periodo_investimento INT,
    multa_cancelamento DECIMAL(5, 2));


CREATE TABLE IF NOT EXISTS clientes (
    client_id INT PRIMARY KEY,
    nome VARCHAR(255),
    sobrenome VARCHAR(255),
    cpf VARCHAR(11),
    rg VARCHAR(10),
    data_nascimento DATE,
    est_civil VARCHAR(15),
    genero VARCHAR(15),
    email VARCHAR(255),
    tel_res VARCHAR(30),
    tel_cel VARCHAR(30),
    endereco VARCHAR(300),
    pais VARCHAR(200),
    cidade VARCHAR(400),
    estado VARCHAR(100),
    cep VARCHAR(10),
    nacionalidade VARCHAR(100),
    renda VARCHAR(100),
    cargo VARCHAR(255),
    tp_cliente VARCHAR(255),
    data_criacao DATE
);


CREATE TABLE IF NOT EXISTS clientes_produtos (
    id SERIAL PRIMARY KEY,
    cliente_id INT,
    produto_id INT,
    data_aquisicao DATE,
    valor_aquisicao DECIMAL(10, 2)
);




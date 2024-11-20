-- Trigger produtos
CREATE OR REPLACE FUNCTION export_csv_produtos_bancarios()
RETURNS TRIGGER AS $$
BEGIN
    -- Gerar o arquivo CSV na pasta compartilhada
    EXECUTE 'COPY (SELECT * FROM produtos_bancarios) TO ''/var/lib/postgresql/exports/produtos_bancarios.csv'' WITH CSV HEADER';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_insert_produtos
AFTER INSERT ON produtos_bancarios
FOR EACH ROW  -- Mudei de FOR EACH STATEMENT para FOR EACH ROW
EXECUTE PROCEDURE export_csv_produtos_bancarios();  -- Chamar a função correta

-- Trigger clientes
CREATE OR REPLACE FUNCTION export_csv_clientes()
RETURNS TRIGGER AS $$
BEGIN
    -- Gerar o arquivo CSV na pasta compartilhada
    EXECUTE 'COPY (SELECT * FROM clientes) TO ''/var/lib/postgresql/exports/clientes.csv'' WITH CSV HEADER';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_insert_clientes
AFTER INSERT ON clientes
FOR EACH ROW  -- Mudei de FOR EACH STATEMENT para FOR EACH ROW
EXECUTE PROCEDURE export_csv_clientes();  -- Chamar a função correta

-- Trigger clientes_produtos
CREATE OR REPLACE FUNCTION export_csv_clientes_produtos()
RETURNS TRIGGER AS $$
BEGIN
    -- Gerar o arquivo CSV na pasta compartilhada
    EXECUTE 'COPY (SELECT * FROM clientes_produtos) TO ''/var/lib/postgresql/exports/clientes_produtos.csv'' WITH CSV HEADER';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_insert_clientes_produtos
AFTER INSERT ON clientes_produtos
FOR EACH ROW  -- Mudei de FOR EACH STATEMENT para FOR EACH ROW
EXECUTE PROCEDURE export_csv_clientes_produtos();  -- Chamar a função correta

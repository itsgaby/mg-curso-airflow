-- Crear tabla 'project'
CREATE TABLE IF NOT EXISTS project (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insertar proyectos de ejemplo
INSERT INTO project (name, status) VALUES 
    ('Data Pipeline A', 'active'),
    ('ETL Workflow B', 'inactive'),
    ('Analytics Job C', 'active');

-- Crear tabla 'task' relacionada a 'project'
CREATE TABLE IF NOT EXISTS task (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insertar tareas de ejemplo
INSERT INTO task (project_id, name, status) VALUES
    (1, 'Extract data from API', 'completed'),
    (1, 'Transform data with Spark', 'in_progress'),
    (2, 'Run ETL pipeline', 'pending'),
    (3, 'Generate monthly report', 'completed'),
    (3, 'Archive old datasets', 'pending');

-- Crear tabla 'launches'
CREATE TABLE IF NOT EXISTS launches (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    net TIMESTAMP
);

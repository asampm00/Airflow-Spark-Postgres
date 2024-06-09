CREATE TABLE IF NOT EXISTS {{params.table_name}} (
  id SERIAL PRIMARY KEY, -- Auto-incrementing integer for unique ID
  name VARCHAR(255) NOT NULL, -- Name of the god
  title TEXT, -- Title or description (text allows longer content)
  "free" VARCHAR(1) DEFAULT '', -- Free status (boolean can also be used)
  "new" VARCHAR(1) DEFAULT '', -- New indicator (boolean can also be used)
  pantheon VARCHAR(255) NOT NULL, -- Pantheon of the god
  pros TEXT, -- Strengths of the god (text allows for multiple strengths)
  "type" TEXT, -- Type of the god (text allows for multiple types)
  role VARCHAR(255) NOT NULL, -- Role of the god in the game
  card VARCHAR(2048) NOT NULL, -- URL of the god card image
  pantheon_EN VARCHAR(255) NOT NULL, -- Pantheon name in English
  god_name_EN VARCHAR(255) NOT NULL, -- God name in English
  role_EN VARCHAR(255) NOT NULL -- Role name in English
);

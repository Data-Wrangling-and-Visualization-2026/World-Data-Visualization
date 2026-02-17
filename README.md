# Interactive 3D World Map Visualization

## Project Overview
We are constructing an interactive 3D chart of a world map with countries colored according to various statistical indicators from a dataset. Users can rotate the Earth sphere and click on a specific country to view detailed information and related plots.

## Team Roles
- **Zamir Safin**: Frontend Visualization (React, D3.js)
- **Denis Beliaev**: Backend Database (Node.js, PostgreSQL)
- **Rustem Gilmetdinov**: Data Pipelining & GenAI Agents (Python, LLMs)

## Technical Stack
- **Frontend**: React
- **Backend**: Node.js
- **Database**: PostgreSQL, SQLite
- **DevOps**: Docker

## Data Strategy
- **Structured Data**: Scrapped from worldometers.info
- **Unstructured Data**: Extracted from PDFs (Energy Inst, WorldBank, UN)
- **Processing**: GenAI agents used for cleaning and microstate filtering.

echo "✅ Populated README.md."

# 7. Populate .env.example
# cat > .env.example << 'EOF'
# Database
# DB_HOST=localhost
# DB_PORT=5432
# DB_USER=postgres
# DB_PASSWORD=your_password_here
# DB_NAME=world_map_db

# API Keys (For GenAI Agents)
# DEEPSEEK_API_KEY=your_key_here
# CLAUDE_API_KEY=your_key_here
# EOF
# echo "✅ Populated .env.example."

# echo ""
# echo "🎉 Project setup complete!"
# echo "📂 Navigate to the project folder: cd $PROJECT_NAME"
# echo "🔍 Next steps: Initialize git, install dep

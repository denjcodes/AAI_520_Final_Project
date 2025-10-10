# Agentic Investment Research System

**Multi-Agent AI System for Comprehensive Stock Analysis**

A sophisticated investment research platform powered by OpenAI's GPT models, featuring specialized agents for market analysis, news sentiment, and automated optimization workflows.

---

## Quick Start

### 1. Install Dependencies

```bash
# Install uv package manager (fast, modern Python package installer)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install packages
uv venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

uv pip install openai yfinance python-dotenv certifi matplotlib numpy seaborn requests pandas
```

### 2. Configure API Keys

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=sk-your-openai-key-here
NEWSAPI_KEY=your-newsapi-key-here
SEC_API_KEY=your-sec-api-key-here
```

**Get API Keys:**
- **OpenAI** (required): https://platform.openai.com/api-keys
- **NewsAPI** (optional): https://newsapi.org/register
- **SEC API** (optional): https://sec-api.io/

### 3. Run the Notebook

```bash
# Recommended: Latest version with clean output
jupyter notebook Agentic_Invest_Research_System_Combined_v1.ipynb
```

### 4. Execute Analysis

Run all cells in order, or jump to the execution cell:

```python
# Analyze any stock
analysis_results = run_investment_research('AAPL')
```

---

## Features

### Multi-Agent Architecture
- **LeadAnalyst**: Strategic planning and coordination
- **MarketAnalyst**: Price action and technical analysis
- **NewsAnalyst**: Sentiment analysis from news sources
- **FundamentalAnalyst**: Financial metrics evaluation

### Advanced Workflows
- **Prompt Chaining**: Sequential analysis pipeline for news processing
- **Agent Routing**: Intelligent task distribution to specialized agents
- **Evaluator-Optimizer**: Iterative quality improvement (3 iterations, 7.0+ quality threshold)
- **Memory System**: Persistent learning across analysis sessions

### Data Sources
- **Yahoo Finance**: Real-time price data and company information
- **NewsAPI**: Current news articles and sentiment
- **SEC Filings**: Regulatory documents and financial statements

---

## Usage Examples

### Analyze Different Stocks

```python
# Tech stocks
run_investment_research('MSFT')  # Microsoft
run_investment_research('GOOGL') # Alphabet
run_investment_research('NVDA')  # NVIDIA

# Other sectors
run_investment_research('JPM')   # Banking
run_investment_research('XOM')   # Energy
```

### Customize Analysis Parameters

```python
# Create coordinator with custom settings
coordinator = InvestmentResearchCoordinator()

# Adjust quality threshold and iterations
coordinator.evaluator = EvaluatorOptimizer(max_iterations=5)
coordinator.evaluator.quality_threshold = 8.5

# Run analysis
results = coordinator.conduct_full_analysis('AAPL')
```

### Access Individual Agents

```python
# Use specialized agents directly
market_analyst = MarketAnalyst()
price_data = market_analyst.analyze_price_action('AAPL')

news_analyst = NewsAnalyst()
sentiment = news_analyst.analyze_sentiment('AAPL')
```

---

## Output Structure

### Analysis Results

```python
{
    'symbol': 'AAPL',
    'timestamp': '2025-01-09T...',
    'components': {
        'plan': {...},              # Strategic execution plan
        'routing': {...},           # Agent routing decisions
        'prompt_chain': {...},      # News analysis pipeline
        'optimized_analysis': {...},# Quality-improved analysis
        'reflection': {...}         # Self-assessment
    },
    'executive_summary': '...',     # Human-readable report
    'metadata': {
        'saved_to': 'investment_analysis_AAPL_*.json'
    }
}
```

### Quality Score Interpretation

| Score | Quality Level | Description |
|-------|--------------|-------------|
| 0-3   | Poor         | Missing critical data or major errors |
| 4-6   | Fair         | Basic analysis, needs improvement |
| 7-8   | Good         | Meets quality threshold, actionable |
| 9-10  | Excellent    | Comprehensive, high-confidence analysis |

---

## Visualizations

The system generates two visualization sets:

**1. Analysis Progress Dashboard (2×2 grid)**
- Quality score improvement over iterations
- Final quality metrics breakdown
- Individual metrics evolution
- Workflow execution summary

**2. Price & Sentiment Charts**
- 6-month price trend with moving averages
- News sentiment distribution (positive/negative/neutral/mixed)

---

## Sample Output

```
Investment Research Agent System
Analyzing: AAPL

Phase 1: Planning
LeadAnalyst created plan with 13 steps

Phase 2: Data Gathering (with Routing)
  Task: Analyze price trends for AAPL...
  -> Routed to MarketPro for price analysis

Phase 3: News Analysis (with Prompt Chaining)
Starting Prompt Chain for AAPL
  1. Ingesting news...
  2. Preprocessing...
  3. Classifying sentiment...
  4. Extracting insights...
  5. Summarizing...
  Prompt chain complete!

Phase 5: Optimization
Starting Evaluator-Optimizer Workflow
  Iteration 1/3
    Quality Score: 4.2/10
    Feedback: ['Add more comprehensive analysis', 'Provide deeper insights']
  Iteration 2/3
    Quality Score: 6.8/10
  Iteration 3/3
    Quality Score: 7.2/10
    Quality threshold met!

============================================================
Executive Summary
============================================================
Investment Analysis for AAPL
Quality Score: 7.2/10

Key Findings:
- Price Trend: Upward momentum with strong support levels
- News Sentiment: Moderately positive (65% positive articles)
- Recommendation: BUY with 12-month target 15% above current

Analysis leveraged 8 prior research experiences

Results saved to: investment_analysis_AAPL_20250109_143022.json
```

---

## Troubleshooting

### API Key Issues
**Problem**: "API key not found"
**Solution**: Verify `.env` file exists in project root with valid keys

### YFinance Errors
**Problem**: Price data fetching fails
**Solution**: System uses fallback stub data. Check internet connection or SSL certificates

### Rate Limits
**Problem**: "OpenAI rate limit exceeded"
**Solution**: Wait a few minutes or upgrade OpenAI plan. The system handles rate limits gracefully

### No News Data
**Problem**: NewsAPI returns empty results
**Solution**:
- Verify NewsAPI key is valid
- Free tier limited to 100 requests/day
- System falls back to stub data if unavailable

---

## Project Structure

### Main Notebooks
- **`Agentic_Invest_Research_System_Combined_v1.ipynb`** (recommended)
  Latest version with clean output, all fixes applied

- **`Agentic_Invest_Research_System_Combined_v0.ipynb`**
  Original version for reference

### Documentation
- **`README.md`** - This file

### Individual Contributions
- **`Agentic_Invest_Research_System_Dennis.ipynb`** - Dennis Arpurayil
- **`Agentic_Invest_Research_System_Divya.ipynb`** - Divya Kamath
- **`Agentic_Invest_Research_System_Pros.ipynb`** - Pros Loung

---

## Technical Details

### System Requirements
- Python 3.8+
- 4GB RAM minimum (8GB recommended)
- Internet connection for API access
- Jupyter Notebook or JupyterLab

### Dependencies
- `openai` - GPT model integration
- `yfinance` - Market data retrieval
- `pandas` - Data processing
- `matplotlib` / `seaborn` - Visualizations
- `python-dotenv` - Environment configuration

### Architecture Patterns
- **Agent-based**: Specialized roles with clear responsibilities
- **Workflow orchestration**: Coordinated multi-step processes
- **Iterative refinement**: Quality-driven optimization loops
- **Memory persistence**: Learning across sessions

---

## Contributing

This is a university course project (AAI-520, University of San Diego). For questions or suggestions:

1. Open an issue with detailed description
2. Reference specific cells or functions
3. Include code examples if applicable

---

## Team

**Course**: AAI-520 - Advanced AI Applications
**University**: University of San Diego
**Team Members**:
- Pros Loung
- Dennis Arpurayil
- Divya Kamath

---

## License

Educational project - see course materials for usage terms.

---

**Version**: 1.0
**Last Updated**: January 2025

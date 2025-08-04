import streamlit as st
import boto3
import json
import time
import os
import re
from uuid import uuid4
from typing import Dict, Any, Optional

# Try to load configuration from config.py if it exists
# try:
from config import (
    AWS_REGION as DEFAULT_AWS_REGION,
    DEFAULT_STATE_MACHINE_ARN,
    DEFAULT_PROMPT_ID,
    SAMPLE_QUERIES
)

# Page configuration
st.set_page_config(
    page_title="Neognemoics Text2SQL Demo",
    page_icon=None,
    layout="wide"
)

def init_aws_client(region: str) -> boto3.client:
    """Initialize AWS Step Functions client using environment variables or IAM role"""
    try:
        # Use environment variables or IAM role
        client = boto3.client('stepfunctions', region_name=region)
        
        # Test the client with a simple call
        client.list_state_machines(maxResults=1)
        return client
    except Exception as e:
        st.error(f"Failed to initialize AWS client: {e}")
        return None

def execute_pipeline(
    client: boto3.client,
    state_machine_arn: str,
    user_query: str,
    session_id: str,
    prompt_id: str
) -> Dict[str, Any]:
    """Execute the Step Functions pipeline"""
    
    # Prepare input for the state machine
    input_data = {
        "body": json.dumps({
            "user_query": user_query,
            "session_id": session_id,
            "prompt_id": prompt_id
        })
    }
    
    try:
        # Start execution
        execution_name = f"user-execution-{int(time.time())}"
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data)
        )
        
        execution_arn = response['executionArn']
        
        # Wait for completion with progress indicator
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        while True:
            status_text.text("Processing your query...")
            execution_result = client.describe_execution(executionArn=execution_arn)
            status = execution_result['status']
            
            if status == 'SUCCEEDED':
                progress_bar.progress(100)
                status_text.text("Query completed successfully!")
                
                # Parse the output
                output = json.loads(execution_result.get('output', '{}'))
                return {
                    'success': True,
                    'status': status,
                    'output': output,
                    'execution_arn': execution_arn
                }
                
            elif status == 'FAILED':
                progress_bar.progress(100)
                status_text.text("Query failed")
                return {
                    'success': False,
                    'status': status,
                    'error': execution_result.get('output', 'Unknown error'),
                    'execution_arn': execution_arn
                }
                
            elif status == 'TIMED_OUT':
                progress_bar.progress(100)
                status_text.text("Query timed out")
                return {
                    'success': False,
                    'status': status,
                    'error': 'Execution timed out',
                    'execution_arn': execution_arn
                }
                
            # Update progress (simple animation)
            for i in range(0, 90, 10):
                progress_bar.progress(i)
                time.sleep(0.5)
                
    except Exception as e:
        st.error(f"Error executing pipeline: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def clean_text_response(text: str) -> str:
    """
    Clean text response to fix character spacing and formatting issues
    
    Args:
        text: Raw text that may have character spacing issues
        
    Returns:
        Cleaned text with proper formatting
    """
    if not isinstance(text, str):
        return text
    
    # Fix character spacing issues like "5 3 . 1 5 m i l l i o n" -> "53.15 million"
    # Pattern: Look for sequences of single characters separated by spaces
    text = re.sub(r'\b(\w)\s+(?=\w\s+\w)', r'\1', text)
    
    # More aggressive fix for character sequences - remove spaces between single chars
    # but preserve word boundaries
    text = re.sub(r'\b(\w)\s+(\w)\s+(\w)', r'\1\2\3', text)
    
    # Fix number formatting like "5 3 . 1 5" -> "53.15"
    text = re.sub(r'(\d)\s+(\d)', r'\1\2', text)
    text = re.sub(r'(\d)\s+(\.)(\s+)(\d)', r'\1\2\4', text)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Fix common currency/number formatting
    text = re.sub(r'(\d)\s*([.,])\s*(\d)', r'\1\2\3', text)
    
    # Clean up extra whitespace at beginning/end
    text = text.strip()
    
    return text

def display_results(result: Dict[str, Any]):
    """Display the pipeline results"""
    
    if not result.get('success', False):
        st.error("Pipeline execution failed")
        if 'error' in result:
            st.error(f"Error: {result['error']}")
        return
    
    output = result.get('output', {})
    
    # Check if we have a successful response
    if output.get('statusCode') == 200:
        body = output.get('body')
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except:
                pass
        
        if isinstance(body, dict) and 'llm_response' in body:
            llm_response = body['llm_response']
            
            st.success("Query processed successfully!")
            
            # Display the main response
            if isinstance(llm_response, dict):
                if 'summary' in llm_response:
                    st.subheader("Summary")
                    st.write(clean_text_response(llm_response['summary']))
                
                # Display SQL Query if available
                if 'sql_query' in body and body['sql_query']:
                    st.subheader("SQL Query")
                    st.code(body['sql_query'], language='sql')
                
                if 'data' in llm_response and llm_response['data']:
                    st.subheader("Data")
                    st.json(llm_response['data'])
                
                if 'sources' in llm_response and llm_response['sources']:
                    st.subheader("Sources")
                    for source in llm_response['sources']:
                        st.write(f"• {clean_text_response(source)}")
                
                if 'links' in llm_response and llm_response['links']:
                    st.subheader("Links")
                    for link in llm_response['links']:
                        st.write(f"• {clean_text_response(link)}")
            else:
                st.write(clean_text_response(llm_response))
        else:
            st.write("Response received but format is unexpected:")
            st.json(body)
    else:
        st.error(f"Pipeline returned status code: {output.get('statusCode')}")
        st.json(output)

def main():
    """Main Streamlit app"""
    
    st.title("NeogenomicsText2SQL Demo")
    st.markdown("Ask questions about your data and get SQL-powered answers!")
    
    # Initialize session ID if not exists
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid4())
    
    # Initialize query text if not exists
    if 'query_text' not in st.session_state:
        st.session_state.query_text = ""
    
    # Session Management Section
    st.header("Session Management")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.info(f"**Current Session ID:** `{st.session_state.session_id}`")
        st.caption("This ID is sent with your query to maintain conversation context across multiple requests.")
    
    with col2:
        if st.button("Reset Session", help="Generate a new session ID to start fresh", type="secondary"):
            old_session = st.session_state.session_id[:8]
            st.session_state.session_id = str(uuid4())
            st.success(f"New session started!\n\nOld: `{old_session}...`\nNew: `{st.session_state.session_id[:8]}...`")
            st.rerun()
    
    with col3:
        if st.button("Copy Session ID", help="Copy full session ID to clipboard"):
            st.code(st.session_state.session_id)
            st.success("Session ID displayed above for copying!")
    
    st.divider()
    
    # Sidebar for AWS configuration
    st.sidebar.header("AWS Configuration")
    
    aws_region = st.sidebar.text_input(
        "AWS Region",
        value=DEFAULT_AWS_REGION,
        help="The AWS region where your Step Functions state machine is deployed"
    )
    
    # AWS Authentication
    st.sidebar.info("Using IAM role or environment variables for AWS authentication")
    
    st.sidebar.subheader("Pipeline Configuration")
    
    state_machine_arn = st.sidebar.text_input(
        "State Machine ARN",
        value=DEFAULT_STATE_MACHINE_ARN,
        placeholder="arn:aws:states:us-east-1:664418979226:stateMachine:Text2SQL-Pipeline-Dev",
        help="The ARN of your Step Functions state machine"
    )
    
    prompt_id = st.sidebar.text_input(
        "Prompt ID",
        value=DEFAULT_PROMPT_ID,
        placeholder="1ffc97b1-0c90-46cc-a9c8-76e5faa4195f",
        help="The ID of the prompt configuration to use"
    )
    
    # Session info in sidebar
    st.sidebar.header("Session Info")
    st.sidebar.text(f"Current Session:")
    st.sidebar.code(st.session_state.session_id[:8] + "...")
    st.sidebar.caption("Full session ID shown in main area")
    
    # Quick query buttons
    st.header("Quick Queries")
    st.caption("Click any button below to load the query into the text area")
    
    cols = st.columns(2)
    for i, query in enumerate(SAMPLE_QUERIES[:4]):  # Show first 4 sample queries
        with cols[i % 2]:
            if st.button(f"{query[:50]}{'...' if len(query) > 50 else ''}", key=f"sample_{i}"):
                st.session_state.query_text = query
                st.success(f"Query loaded: '{query[:60]}{'...' if len(query) > 60 else ''}'")
                st.rerun()
    
    # Main interface
    st.header("Ask Your Question")
    
    user_query = st.text_area(
        "Enter your question:",
        value=st.session_state.query_text,
        placeholder="What are the unique gene names?",
        height=100,
        key="user_query_input"
    )
    
    # Update session state when text area changes
    if user_query != st.session_state.query_text:
        st.session_state.query_text = user_query
    
    # Clear query button
    if st.session_state.query_text:
        if st.button("Clear Query", help="Clear the current query text"):
            st.session_state.query_text = ""
            st.rerun()
    
    # Execute query
    if st.button("Execute Query", type="primary", disabled=not user_query.strip()):
        
        # Validate inputs
        if not state_machine_arn:
            st.error("Please provide the State Machine ARN in the sidebar")
            return
        
        if not prompt_id:
            st.error("Please provide a Prompt ID in the sidebar")
            return
        
        # Show session info being used
        st.info(f"Using Session ID: `{st.session_state.session_id[:8]}...` for this query")
        
        # Initialize AWS client
        client = init_aws_client(region=aws_region)
        
        if not client:
            st.error("Could not connect to AWS. Please check your credentials and region.")
            return
        
        # Execute the pipeline
        with st.spinner("Processing your query..."):
            result = execute_pipeline(
                client=client,
                state_machine_arn=state_machine_arn,
                user_query=user_query.strip(),
                session_id=st.session_state.session_id,
                prompt_id=prompt_id
            )
        
        # Display results
        display_results(result)
        
        # Show execution details in expander
        if 'execution_arn' in result:
            with st.expander("Execution Details"):
                st.text(f"Execution ARN: {result['execution_arn']}")
                st.text(f"Status: {result.get('status', 'Unknown')}")
                st.text(f"Session ID used: {st.session_state.session_id}")
                
                # AWS Console link
                region = aws_region
                console_url = f"https://{region}.console.aws.amazon.com/states/home?region={region}#/executions/details/{result['execution_arn']}"
                st.markdown(f"[View in AWS Console]({console_url})")
    
    # Configuration help
    with st.expander("Need Help Getting Started?"):
        st.write("""
        **To get your State Machine ARN:**
        ```bash
        aws stepfunctions list-state-machines --region us-east-1
        ```
        
        **To get your Prompt IDs:**
        ```bash
        aws dynamodb scan --table-name your-prompts-table --region us-east-1
        ```
        
        **About Session Management:**
        - Session IDs help maintain conversation context across multiple queries
        - Each session ID is a unique UUID that gets passed to your Step Functions pipeline
        - Reset your session when you want to start a completely new conversation
        - The same session ID will be reused until you reset it
        
        **Using Quick Queries:**
        - Click any quick query button to load it into the text area
        - You can then edit the query before executing it
        - Use the "Clear Query" button to start fresh
        
        **AWS Authentication:**
        The app uses IAM roles or environment variables for AWS authentication when deployed in AWS.
        
        **Tip:** Create a `config.py` file (see `config_template.py`) to avoid entering configuration values every time!
        """)
# comment for test
if __name__ == "__main__":
    main()

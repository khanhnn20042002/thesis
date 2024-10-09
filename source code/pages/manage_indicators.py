import streamlit as st
from code_editor import code_editor
from jinja2 import Environment, FileSystemLoader
import os
import json
import utils
from st_pages import Page, show_pages

st.set_page_config(layout='centered')

show_pages(
    [
        Page("dashboard.py", "Dashboard"),
        Page("pages/manage_indicators.py", "Manage your indicators"),
    ]
)

st.session_state.indicators = st.session_state.get("indicators", [])
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

@st.cache_resource
def get_template():
    env = Environment(loader=FileSystemLoader("jinja-templates/"))
    return env.get_template("custom-indicator.py.jinja2")

tpl= get_template()

def create_indicator(user_code, output_variable, indicator):
    if indicator in st.session_state.indicators:
        st.error(f'{indicator} already exists', icon="🚨")
        return None
    if not utils.validate_python_syntax(user_code):
        st.error('Your code contains a syntax error', icon="🚨")
        return None
    if not utils.check_variable_in_code(user_code, output_variable):
        st.error(f'{output_variable} variable is not in your code', icon="🚨")
        return None
    variables = {'user_code': user_code,
                 'output_variable': output_variable,
                 'indicator': indicator}
    json.dump(variables, open(os.path.join(parent_directory, f'custom-indicators/{indicator}.json'), 'w'))
    code = tpl.render(variables)
    with open(os.path.join(parent_directory, f'custom-indicators/{indicator}.py'), 'w') as f:
        f.write(code)
    st.session_state.indicators.append(indicator) 
    st.success(f'{indicator} is created successfully', icon="✅")
   

def update_indicator(user_code, output_variable, indicator):
    if not utils.validate_python_syntax(user_code):
        st.error('Your code contains a syntax error', icon="🚨")
        return None
    if not utils.check_variable_in_code(user_code, output_variable):
        st.error(f'{output_variable} is not in your code', icon="🚨")
        return None
    variables = {'user_code': user_code,
                 'output_variable': output_variable,
                 'indicator': indicator}
    json.dump(variables, open(os.path.join(parent_directory, f'custom-indicators/{indicator}.json'), 'w'))
    code = tpl.render(variables)
    with open(os.path.join(parent_directory, f'custom-indicators/{indicator}.py'), 'w') as f:
        f.write(code)
    st.success(f'{indicator} is updated successfully', icon="✅")

def delete_indicator(indicator):
    os.remove(os.path.join(parent_directory, f'custom-indicators/{indicator}.py'))
    os.remove(os.path.join(parent_directory, f'custom-indicators/{indicator}.json'))
    st.session_state.indicators.remove(indicator)
    if f'{indicator}.csv' in os.listdir(os.path.join(parent_directory, 'tmp')):
        os.remove(os.path.join(parent_directory, f'tmp/{indicator}.csv'))
    st.success(f'{indicator} is deleted successfully', icon="✅")

with st.sidebar:
    st.subheader("Your Indicators")
    for indicator in st.session_state.indicators:
        st.markdown(f"- {indicator}")
    st.subheader("Chose an operation")
    operation = st.selectbox("choose an operation", ["create", "update", "delete"], label_visibility="collapsed")

if operation == "create":
    st.title("Create your indicator")
    
    st.subheader("Name")
    indicator = st.text_input('indicator', placeholder='Enter your indicator name here...', label_visibility="collapsed")

    st.subheader('Python code')
    user_code = code_editor('', 'python', response_mode="blur", height="300px")["text"]

    st.subheader('Output')
    output_variable = st.text_input('output_variable', placeholder='Enter your output variable here...', label_visibility="collapsed")

    st.button("create", on_click=create_indicator, args=(user_code, output_variable, indicator))
elif operation == "update":
    updated_indicator =  st.selectbox("updated_indicator", ["---Choose a indicator to update---"] + st.session_state.indicators, label_visibility="collapsed")
    if updated_indicator != "---Choose a indicator to update---":
        variables = json.load(open(os.path.join(parent_directory, f'custom-indicators/{updated_indicator}.json')))
        user_code_value = variables["user_code"]
        indicator_value = variables["indicator"]
        output_variable_value = variables["output_variable"]

        st.subheader("Name")
        indicator = st.text(indicator_value)

        st.subheader('Python code')
        user_code = code_editor(user_code_value, 'python', response_mode="blur", height="300px")["text"]

        st.subheader('Output')
        output_variable = st.text_input('output_variable', placeholder='Enter your output variable here...', label_visibility="collapsed",value = output_variable_value)

        st.button("update", on_click=update_indicator, args=(user_code, output_variable, indicator))
elif operation == "delete":
    deleted_indicator =  st.selectbox("deleted_indicator", ["---Choose a indicator to delete---"] + st.session_state.indicators, label_visibility="collapsed")
    if deleted_indicator != "---Choose a indicator to delete---":
        st.button("delete", on_click = delete_indicator, args=(deleted_indicator, ))

    




from fabric import fabricclient

def upload_to_onelake(file_name, workspace_name, lakehouse_name):
    fabric_client = fabricclient.FabricClient()
    fabric_client.authenticate()
    workspace = fabric_client.workspaces.get(workspace_name)
    lakehouse = workspace.lakehouses.get(lakehouse_name)
    lakehouse.files.upload(
        local_path=file_name,
        remote_path=f"Files/raw/connectwise/{file_name}"
    )
    print(f"Successfully uploaded {file_name} to OneLake")

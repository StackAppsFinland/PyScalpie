import requests
import json


class OneDrive:
    def __init__(self, client_id, client_secret, redirect_uri, refresh_token=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
        self.api_base_url = "https://graph.microsoft.com/v1.0/me/drive/root:"
        self.session = requests.Session()

        if refresh_token:
            self.refresh_token = refresh_token
            self.refresh_access_token()
        else:
            self.access_token = None

    def refresh_access_token(self):
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': self.redirect_uri
        }
        response = self.session.post(self.token_url, data=data)
        response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
        self.access_token = response.json().get('access_token')
        self.session.headers.update({'Authorization': f'Bearer {self.access_token}'})

    def get_file(self, file_path):
        response = self.session.get(f'{self.api_base_url}/{file_path}:/content')
        response.raise_for_status()  # Check if the request was successful
        return response.content  # Return file content

    def save_file(self, file_path, content):
        response = self.session.put(f'{self.api_base_url}/{file_path}:/content', data=content)
        response.raise_for_status()  # Check if the request was successful
        return response.json()  # Return the JSON response

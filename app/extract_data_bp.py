import requests

def get_api_data(url):
    """
    Fetch data from a given API endpoint.
    Includes error handling for network issues, HTTP errors, and unexpected responses.
    """
    try:
        # Send a GET request to the API
        response = requests.get(url, timeout=10)
        
        # Raise an exception if the HTTP status code is not 200 (OK)
        response.raise_for_status()

        # Try to parse the response as JSON
        data = response.json()

        # Return the parsed data
        return data

    except requests.exceptions.Timeout:
        # Handle timeout errors (e.g., when the API takes too long to respond)
        print("Error: The request timed out.")
    except requests.exceptions.ConnectionError:
        # Handle network errors (e.g., no internet connection)
        print("Error: Failed to connect to the API.")
    except requests.exceptions.HTTPError as http_err:
        # Handle specific HTTP errors (e.g., 404 Not Found, 500 Internal Server Error)
        print(f"HTTP error occurred: {http_err}")
    except ValueError:
        # Handle errors when parsing JSON fails
        print("Error: The response is not valid JSON.")
    except Exception as err:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred: {err}")

    # Return None if any error occurs
    return None


if __name__ == "__main__":
    # Example API endpoint (public placeholder API)
    api_url = "https://raw.githubusercontent.com/estelaromer/csv-examples/refs/heads/main/data.json"

    # Fetch data from the API
    result = get_api_data(api_url)

    # Check if data was successfully retrieved
    if result is not None:
        print("Data retrieved successfully!")
        # Print the first 3 items for demonstration
        print(result["company"]["name"])
    else:
        print("Failed to retrieve data from the API.")

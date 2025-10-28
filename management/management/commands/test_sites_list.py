from django.core.management.base import BaseCommand
from management.utils import fetch_user_sites_list


class Command(BaseCommand):
    help = 'Test fetch_user_sites_list function with debug output'

    def add_arguments(self, parser):
        parser.add_argument(
            '--email',
            type=str,
            help='Email address to test with',
            required=True
        )

    def handle(self, *args, **options):
        email = options['email']
        self.stdout.write(f"Testing fetch_user_sites_list with email: {email}")
        
        try:
            result = fetch_user_sites_list(email)
            self.stdout.write(f"Result type: {type(result)}")
            self.stdout.write(f"Result: {result}")
            
            if isinstance(result, dict):
                if 'status' in result:
                    self.stdout.write(f"Status: {result['status']}")
                if 'sites' in result:
                    self.stdout.write(f"Number of sites: {len(result['sites'])}")
                    for i, site in enumerate(result['sites'][:5]):  # Show first 5
                        self.stdout.write(f"Site {i+1}: {site}")
                if 'error' in result:
                    self.stdout.write(f"Error: {result['error']}")
            elif isinstance(result, list):
                self.stdout.write(f"Number of sites: {len(result)}")
                for i, site in enumerate(result[:5]):  # Show first 5
                    self.stdout.write(f"Site {i+1}: {site}")
                    
        except Exception as e:
            self.stdout.write(f"Exception occurred: {str(e)}")
            import traceback
            self.stdout.write(traceback.format_exc())
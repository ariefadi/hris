# Django Settings Update for New Template Structure

## Required Changes to settings.py

To support the new template structure, you need to update your Django `TEMPLATES` configuration in `hris/settings.py`:

### Current Structure
The templates are now organized as follows:
```
hris/
├── templates/
│   ├── base/
│   │   ├── base.html                 # Main base template
│   │   ├── admin_base.html           # Admin-specific base template
│   │   └── components/
│   │       ├── navbar.html           # Navigation bar component
│   │       ├── sidebar.html          # Sidebar component
│   │       └── footer.html           # Footer component
│   └── admin/
│       └── admin_layout.html         # Admin layout template
└── management/
    └── templates/
        └── admin/                    # App-specific templates
            ├── dashboard_admin.html
            ├── login_admin.html
            └── [other admin templates...]
```

### Required TEMPLATES Configuration

Update your `TEMPLATES` setting in `hris/settings.py`:

```python
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            BASE_DIR / 'templates',  # Project-level templates
            BASE_DIR / 'management' / 'templates',  # Management app templates
            # Add other app template directories as needed:
            # BASE_DIR / 'tasks' / 'templates',
            # BASE_DIR / 'system' / 'templates',
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]
```

### Benefits of This Structure

1. **Scalability**: Easy to add new apps (tasks, system, etc.) without template conflicts
2. **Reusability**: Shared components can be used across all apps
3. **Maintainability**: Base templates are centralized and easy to update
4. **Organization**: Clear separation between project-level and app-specific templates

### Template Inheritance Chain

```
base/base.html
└── base/admin_base.html
    └── admin/admin_layout.html
        └── [app-specific templates]
```

### Components Usage

All admin templates now automatically include:
- Responsive navbar with portal dropdown
- Sidebar with navigation menu
- Footer with copyright information
- All necessary CSS and JavaScript libraries

### Next Steps

1. Update your `hris/settings.py` with the TEMPLATES configuration above
2. Test the application to ensure all templates load correctly
3. Future apps should follow this structure:
   ```
   new_app/
   └── templates/
       └── new_app/
           └── [app templates extending admin/admin_layout.html]
   ```
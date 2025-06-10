# dukdb-go Documentation

[![Built with Starlight](https://astro.badg.es/v2/built-with-starlight/tiny.svg)](https://starlight.astro.build)

Comprehensive documentation for the **dukdb-go** pure-Go DuckDB driver.

## Documentation Structure

This documentation site provides complete coverage of the dukdb-go driver:

### Getting Started

- **Introduction** - Overview and key features
- **Installation & Setup** - Getting the driver installed and running
- **Build Process** - Building from source and deployment

### User Guides

- **Working with Complex Types** - LIST, STRUCT, MAP, UUID types
- **Performance Optimization** - Connection pooling, caching, optimization tips
- **Migration from CGO** - Step-by-step migration from the CGO driver
- **Code Examples** - Complete working examples for all features
- **Troubleshooting** - Common issues and solutions

### API Reference

- **Driver API** - Complete API documentation

## Local Development

### Prerequisites

- Node.js 18+ and npm
- Or use Nix: `nix develop`

### Commands

All commands are run from the `doc/` directory:

| Command | Action |
|---------|--------|
| `npm install` | Install dependencies |
| `npm run dev` | Start local dev server at `localhost:4321` |
| `npm run build` | Build production site to `./dist/` |
| `npm run preview` | Preview build locally |

### Using Nix

The documentation includes a Nix flake for reproducible development:

```bash
# Enter development environment
nix develop

# Start development server
npm run dev
```

## Content Organization

```
src/content/docs/
├── index.mdx                    # Homepage
├── guides/
│   ├── getting-started.md       # Installation and basic usage
│   ├── build-process.md         # Build instructions and deployment
│   ├── complex-types.md         # Advanced DuckDB types
│   ├── performance.md           # Optimization techniques
│   ├── migration.md             # CGO to pure-Go migration
│   ├── examples.md              # Complete code examples
│   └── troubleshooting.md       # Common issues and solutions
└── reference/
    └── api.md                   # Complete API reference
```

## Features

### Documentation Features

- **Comprehensive Coverage** - All driver features documented
- **Code Examples** - Working examples for every feature
- **Performance Guides** - Optimization and best practices
- **Migration Support** - Detailed migration from CGO driver
- **Search** - Full-text search across all documentation
- **Mobile Responsive** - Optimized for all devices
- **Dark/Light Mode** - Automatic theme switching

### Technical Features

- Built with [Astro](https://astro.build/) and [Starlight](https://starlight.astro.build/)
- Markdown/MDX content with component support
- Syntax highlighting for Go, SQL, and other languages
- Automatic navigation generation
- SEO optimized
- Fast static site generation

## Contributing

Documentation improvements are welcome! To contribute:

1. Fork the repository
1. Make your changes in the `doc/src/content/docs/` directory
1. Test locally with `npm run dev`
1. Submit a pull request

### Writing Guidelines

- Use clear, concise language
- Include working code examples
- Add proper syntax highlighting
- Test all code examples
- Follow the existing structure and style

### Content Standards

- **Getting Started**: Should get users running quickly
- **Guides**: Step-by-step instructions with examples
- **Reference**: Complete API documentation with all parameters
- **Examples**: Full, working programs that demonstrate features

## Deployment

The documentation is automatically deployed when changes are merged to the main branch.

### Manual Deployment

```bash
# Build for production
npm run build

# Preview the build
npm run preview

# Deploy to your hosting platform
```

## Links

- **Main Repository**: [github.com/connerohnesorge/dukdb-go](https://github.com/connerohnesorge/dukdb-go)
- **Issues**: [Report documentation issues](https://github.com/connerohnesorge/dukdb-go/issues)
- **DuckDB**: [Official DuckDB documentation](https://duckdb.org/docs/)

## License

This documentation is part of the dukdb-go project and follows the same license terms.

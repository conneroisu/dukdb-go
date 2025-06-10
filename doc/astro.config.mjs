// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'dukdb-go Documentation',
			description: 'Pure-Go DuckDB driver documentation',
			social: [
				{ 
					icon: 'github', 
					label: 'GitHub', 
					href: 'https://github.com/connerohnesorge/dukdb-go' 
				}
			],
			editLink: {
				baseUrl: 'https://github.com/connerohnesorge/dukdb-go/edit/main/doc/',
			},
			customCss: [
				// Custom CSS for better code highlighting
				'./src/styles/custom.css',
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: 'index' },
						{ label: 'Installation & Setup', slug: 'guides/getting-started' },
						{ label: 'Build Process', slug: 'guides/build-process' },
					],
				},
				{
					label: 'User Guides',
					items: [
						{ label: 'Working with Complex Types', slug: 'guides/complex-types' },
						{ label: 'Performance Optimization', slug: 'guides/performance' },
						{ label: 'Migration from CGO', slug: 'guides/migration' },
						{ label: 'Code Examples', slug: 'guides/examples' },
						{ label: 'Troubleshooting', slug: 'guides/troubleshooting' },
					],
				},
				{
					label: 'API Reference',
					items: [
						{ label: 'Driver API', slug: 'reference/api' },
					],
				},
			],
			head: [
				{
					tag: 'meta',
					attrs: {
						name: 'keywords',
						content: 'DuckDB, Go, database, driver, pure-go, analytics, columnar, SQL',
					},
				},
			],
		}),
	],
});

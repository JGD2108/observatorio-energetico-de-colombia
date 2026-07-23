import { defineConfig } from 'astro/config';

export default defineConfig({
  output: 'static',
  site: 'https://observatorio-energetico.vercel.app',
  build: {
    assets: 'assets'
  }
});

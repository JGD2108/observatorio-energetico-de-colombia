import { defineConfig } from 'astro/config';

export default defineConfig({
  output: 'static',
  site: 'https://jgd2108.github.io',
  base: '/observatorio-energetico-de-colombia',
  build: {
    assets: 'assets'
  }
});

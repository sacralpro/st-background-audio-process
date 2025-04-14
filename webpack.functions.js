const nodeExternals = require('webpack-node-externals');

module.exports = {
  mode: 'production',
  externalsPresets: { node: true },
  externals: [nodeExternals({
    // Allow bundling these packages from node_modules
    allowlist: ['node-appwrite'] 
  })],
  module: {
    rules: [
      {
        test: /\.js$/,
        // Process all JavaScript files including those in node_modules/node-appwrite
        exclude: /node_modules\/(?!node-appwrite)/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: [
              ['@babel/preset-env', { targets: { node: '16' } }]
            ],
            plugins: [
              '@babel/plugin-transform-class-properties',
              '@babel/plugin-transform-private-methods',
              '@babel/plugin-transform-class-static-block',
              '@babel/plugin-transform-private-property-in-object'
            ]
          }
        }
      }
    ]
  }
}; 
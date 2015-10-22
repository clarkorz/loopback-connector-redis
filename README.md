## loopback-connector-redis

![StrongLoop Labs](http://docs.strongloop.com/download/thumbnails/5310165/StrongLoop%20Labs%20Logo%20Cropped.png "StrongLoop Labs")

> StrongLoop Labs projects provide early access to advanced or experimental functionality.  In general, these projects may lack usability, completeness, documentation, and robustness, and may be outdated.
However, StrongLoop supports these projects: Paying customers can open issues using the StrongLoop customer support system (Zendesk), and community users can report bugs on GitHub.

**NOTE: THIS MODULE IS PRE-RELEASE AND UNDER ACTIVE DEVELOPMENT**

Redis connector for LoopBack.

## Usage

To use it you need `loopback-datasource-juggler@1.0.x`.

1. Setup dependencies in `package.json`:

    ```json
    {
      ...
      "dependencies": {
        "loopback-datasource-juggler": "~1.0.0",
        "loopback-connector-redis": "latest"
      },
      ...
    }
    ```

2. Use:

    ```javascript
        var DataSource = require('loopback-datasource-juggler').DataSource;
        var ds = new DataSource('redis');
    ```

## Running tests

Make sure you have redis server running on default port, then run

    npm test

Be careful, it could delete your data in database number 0

## MIT License


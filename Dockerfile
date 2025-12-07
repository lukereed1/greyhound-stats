FROM node:18-slim

WORKDIR /app


COPY package*.json ./
RUN npm install


COPY . .


EXPOSE 3000

RUN npm install tsx --save-prod

CMD [ "npm", "start" ]